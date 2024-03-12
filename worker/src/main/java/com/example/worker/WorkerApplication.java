package com.example.worker;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilder;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.integration.partition.StepExecutionRequestHandler;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.messaging.MessageChannel;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Map;

@ImportRuntimeHints(WorkerApplication.Hints.class)
@SpringBootApplication
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }

    static class Hints implements RuntimeHintsRegistrar {

        @Override
        public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
            var mcs = MemberCategory.values();
            hints.reflection().registerType(StepExecutionRequestHandler.class, mcs);
            hints.serialization().registerType(StepExecutionRequest.class);
        }
    }

}

@Configuration
class WorkerConfiguration {

    private final RowMapper<Customer> customerRowMapper =
            (rs, rowNum) -> new Customer(rs.getLong("id"), rs.getString("name"));

    @Bean
    Step workerStep(
            JobExplorer explorer,
            JobRepository repository,
            BeanFactory beanFactory,
            PlatformTransactionManager transactionManager,
            MessageChannel requests,
            MessageChannel replies,
            ItemReader<Customer> customerItemReader) {

        return new RemotePartitioningWorkerStepBuilder("workerStep", repository)
                .inputChannel(requests)
                .outputChannel(replies)
                .jobExplorer(explorer)
                .beanFactory(beanFactory)
                .<Customer, Customer>chunk(3, transactionManager)
                .reader(customerItemReader)
                .writer(chunk -> {
                    for (var c : chunk.getItems())
                        System.out.println(c.id() + ":" + c.name());
                })
                .build();
    }

    @Bean
    @StepScope
    JdbcPagingItemReader<Customer> itemReader(
            DataSource dataSource,
            @Value("#{stepExecutionContext['partition']}") String partition
    ) {
        return new JdbcPagingItemReaderBuilder<Customer>()
                .dataSource(dataSource)
                .name("itemReader")
                .selectClause("SELECT *")
                .fromClause("FROM customer")
                .whereClause("WHERE id in (select b.id from customer_job_buckets b where b.bucket =  :partition ) ")
                .parameterValues(Map.of("partition", partition ))
                .sortKeys(Map.of("id", Order.ASCENDING))
                .rowMapper(this.customerRowMapper)
                .pageSize(10) // Adjust based on your needs
                .build();
    }
}

record Customer(Long id, String name) {
}

@Configuration
class IntegrationConfiguration {

    @Bean
    DirectChannelSpec replies() {
        return MessageChannels.direct();
    }

    @Bean
    DirectChannelSpec requests() {
        return MessageChannels.direct();
    }

    @Bean
    IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow
                .from(replies())
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(
                        "replies"))
                .get();
    }

    @Bean
    IntegrationFlow inboundFlow(ConnectionFactory connectionFactory) {
        var simpleMessageConverter = new SimpleMessageConverter();
        simpleMessageConverter.addAllowedListPatterns("*");
        return IntegrationFlow
                .from(Amqp
                        .inboundAdapter(connectionFactory, "requests")
                        .messageConverter(simpleMessageConverter))
                .channel(requests())
                .get();
    }

}