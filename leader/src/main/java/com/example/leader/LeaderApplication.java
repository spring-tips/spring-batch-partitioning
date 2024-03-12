package com.example.leader;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilder;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.messaging.MessageChannel;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@ImportRuntimeHints(LeaderApplication.Hints.class)
public class LeaderApplication {

    public static void main(String[] args) {
        SpringApplication.run(LeaderApplication.class, args);
    }

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            var mcs = MemberCategory.values();
            hints.reflection().registerType(MessageChannelPartitionHandler.class, mcs);
            hints.serialization().registerType(StepExecutionRequest.class);
        }
    }


    @Bean
    Job remotePartitioningJob(JobRepository jobRepository, Step managerStep) {
        return new JobBuilder("remotePartitioningJob", jobRepository)//
                .incrementer(new RunIdIncrementer())//
                .start(managerStep)//
                .build();
    }

    static class RangePartitioner implements Partitioner {

        private final JdbcClient db;

        RangePartitioner(JdbcClient db) {
            this.db = db;
        }

        @Override
        public Map<String, ExecutionContext> partition(int gridSize) {
            var bucketsDdl = """
                    insert into customer_job_buckets( id, bucket)  
                    SELECT id , 'partition' || NTILE(?) OVER (ORDER BY id ) AS bucket
                    FROM customer ;
                    """;
            this.db.sql(bucketsDdl).params(gridSize).update();
            var partitions = new HashMap<String, ExecutionContext>();
            for (var i = 0; i < gridSize; i++) {
                var context = new ExecutionContext();
                var partitionName = "partition" + (i+1);
                context.putString("partition", partitionName);
                partitions.put(partitionName, context);
            }
            return partitions;
        }
    }

    @Bean
    RangePartitioner rangePartitioner(JdbcClient jdbcClient) {
        return new RangePartitioner(jdbcClient);
    }

    @Bean
    Step managerStep(MessageChannel requests, MessageChannel replies, BeanFactory beanFactory,
                     JobRepository repository, RangePartitioner partitioner) {
        return new RemotePartitioningManagerStepBuilder("partitioningStep", repository)
                .beanFactory(beanFactory)
                .partitioner("workerStep", partitioner)
                .gridSize(3)
                .outputChannel(requests)
                .inputChannel(replies)
                .build();
    }

}

@Configuration
class IntegrationConfiguration {

    @Bean
    IntegrationFlow inboundFlow(MessageChannel replies, ConnectionFactory connectionFactory) {
        var simpleMessageConverter = new SimpleMessageConverter();
        simpleMessageConverter.addAllowedListPatterns("*");
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, "replies")
                        .messageConverter(simpleMessageConverter))
                .channel(replies)
                .get();
    }

    @Bean
    IntegrationFlow outboundFlow(MessageChannel requests, AmqpTemplate amqpTemplate) {
        return IntegrationFlow
                .from(requests)
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey("requests"))
                .get();
    }


    @Bean
    DirectChannelSpec requests() {
        return MessageChannels.direct();
    }

    @Bean
    DirectChannelSpec replies() {
        return MessageChannels.direct();
    }

}

@Configuration
class RabbitMqConfiguration {

    @Bean
    InitializingBean rabbitMqInitializer(AmqpAdmin amqp) {
        return () -> {
            for (var qName : "requests,replies".split(",")) {
                var exchange = ExchangeBuilder.directExchange(qName).build();
                var q = QueueBuilder.durable(qName).build();
                var binding = BindingBuilder
                        .bind(q)
                        .to(exchange)
                        .with(qName)
                        .noargs();
                amqp.declareQueue(q);
                amqp.declareExchange(exchange);
                amqp.declareBinding(binding);
            }
        };
    }
}