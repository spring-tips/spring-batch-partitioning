package com.example.leader;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilder;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.messaging.MessageChannel;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
public class LeaderApplication {

    public static void main(String[] args) {
        SpringApplication.run(LeaderApplication.class, args);
    }

}

@Configuration
class LeaderConfiguration {

    private final RemotePartitioningManagerStepBuilder builder;

    LeaderConfiguration(JobRepository repository,
                        BeanFactory beanFactory) {
        this.builder = new RemotePartitioningManagerStepBuilder(
                "partitioningStep", repository)
                .beanFactory(beanFactory);
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
            // Example partition logic
            var count = db.sql("select count(id) from customer").query(Integer.class).optional().orElse(0);
            var partitions = new HashMap<String, ExecutionContext>();
            var itemsPerPartition = count / gridSize;
            var extra = count % gridSize;
            var start = db.sql("select min( id) from customer").query(Integer.class).optional().orElse(0);
            var end = itemsPerPartition - 1;
            for (var i = 0; i < gridSize; i++) {
                var context = new ExecutionContext();
                if (i == gridSize - 1) {
                    end += extra; // Add the remainder to the last partition
                }
                context.putInt("start", start);
                context.putInt("end", end);
                partitions.put("partition" + i, context);
                start = end + 1;
                end += itemsPerPartition;
            }
            return partitions;
        }
    }


    @Bean
    RangePartitioner rangePartitioner(JdbcClient jdbcClient) {
        return new RangePartitioner(jdbcClient);
    }

    @Bean
    Step managerStep(MessageChannel requests, MessageChannel replies, RangePartitioner partitioner) {
        return this.builder
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
    IntegrationFlow inboundFlow(ConnectionFactory connectionFactory) {
        var simpleMessageConverter = new SimpleMessageConverter();
        simpleMessageConverter.addAllowedListPatterns("*");
        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, "replies")
                        .messageConverter(simpleMessageConverter))
                .channel(replies())
                .get();
    }

    @Bean
    IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow
                .from(requests())
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

    private final String[] destinationNames = "requests,replies"
            .split(",");

    @Bean
    InitializingBean rabbitMqInitializer(AmqpAdmin amqp) {
        return () -> {
            for (var qName : destinationNames) {
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