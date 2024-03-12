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

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            var mcs = MemberCategory.values();
            hints.reflection().registerType(MessageChannelPartitionHandler.class, mcs);
            hints.serialization().registerType(StepExecutionRequest.class);
        }
    }

    public static void main(String[] args) {
        SpringApplication.run(LeaderApplication.class, args);
    }


    private static final int GRID_SIZE = 3;

    static class SpringTipsPartitioner implements Partitioner {

        private final JdbcClient db;

        SpringTipsPartitioner(JdbcClient db) {
            this.db = db;
        }

        @Override
        public Map<String, ExecutionContext> partition(int gridSize) {

            var sql = """
                                        
                    insert into customer_job_buckets(customer_id, bucket)
                    select id, 'partition' || NTILE(?) over (order by id) as bucket 
                    from customer
                    """;
            this.db.sql(sql).params(gridSize).update();

            var partitions = new HashMap<String, ExecutionContext>();
            for (var i = 0; i < gridSize; i++) {
                var partitionName = "partition" + (i + 1);

                var context = new ExecutionContext();
                context.putString("partition", partitionName);

                partitions.put(partitionName, context);
            }

            return partitions;
        }

    }

    @Bean
    SpringTipsPartitioner partitioner(JdbcClient jdbcClient) {
        return new SpringTipsPartitioner(jdbcClient);
    }

    @Bean
    IntegrationFlow outboundFlow(MessageChannel requests, AmqpTemplate amqpTemplate) {
        return IntegrationFlow
                .from(requests)
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey("requests"))
                .get();
    }

    @Bean
    IntegrationFlow inboundFlow(ConnectionFactory cf, MessageChannel replies) {
        var converters = new SimpleMessageConverter();
        converters.addAllowedListPatterns("*");
        return IntegrationFlow
                .from(Amqp.inboundAdapter(cf, "replies").messageConverter(converters))
                .channel(replies)
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

    @Bean
    Step managerStep(JobRepository repository, BeanFactory beanFactory, Partitioner partitioner,
                     MessageChannel requests, MessageChannel replies) {
        return new RemotePartitioningManagerStepBuilder("managerStep", repository)
                .beanFactory(beanFactory)
                .partitioner("workerStep", partitioner)
                .gridSize(GRID_SIZE)
                .outputChannel(requests)
                .inputChannel(replies)
                .build();
    }

    @Bean
    Job remotePartitioningJob(JobRepository repository, Step managerStep) {
        return new JobBuilder("remotePartitioningJob", repository)
                .incrementer(new RunIdIncrementer())
                .start(managerStep)
                .build();
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