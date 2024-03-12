package bootiful.batch.leader;

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
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.integration.partition.MessageChannelPartitionHandler;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilder;
import org.springframework.batch.integration.partition.StepExecutionRequest;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportRuntimeHints;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;

import java.util.Map;

@ImportRuntimeHints(LeaderApplication.Hints.class)
@SpringBootApplication
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

}

@Configuration
class LeaderConfiguration {

    private static final int GRID_SIZE = 3;

    private final RemotePartitioningManagerStepBuilder remotePartitioningManagerStepBuilder;

    LeaderConfiguration(JobRepository repository, BeanFactory beanFactory) {
        this.remotePartitioningManagerStepBuilder = new RemotePartitioningManagerStepBuilder(
                "remote-partitioning-step", repository)
                .beanFactory(beanFactory);
    }


    @Bean
    DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    IntegrationFlow outboundFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow
                .from(requests())
                .handle((payload, headers) -> {
                    // todo setup a corresponding worker node on k8s
                    System.out.println("\tabout to send " + payload + " to requests channel");
                    headers.forEach((key, value) -> System.out.println("\t" + key + '=' + value));
                    return payload;
                })
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(RabbitConfiguration.REQUESTS))
                .get();
    }

    @Bean
    DirectChannel replies() {
        return new DirectChannel();
    }

    @Bean
    IntegrationFlow inboundFlow(ConnectionFactory connectionFactory) {

        var simpleMessageConverter = new SimpleMessageConverter();
        simpleMessageConverter.addAllowedListPatterns("*");

        return IntegrationFlow
                .from(Amqp.inboundAdapter(connectionFactory, RabbitConfiguration.REPLIES).messageConverter(simpleMessageConverter))
                .channel(replies())
                .get();
    }

    @Bean
    Step managerStep() {
        return this.remotePartitioningManagerStepBuilder
                .partitioner("workerStep", new SimplePartitioner())
                .gridSize(GRID_SIZE)
                .outputChannel(requests())
                .inputChannel(replies())
                .build();
    }

    @Bean
    Job remotePartitioningJob(JobRepository jobRepository) {
        return new JobBuilder("remotePartitioningJob", jobRepository)//
                .incrementer(new RunIdIncrementer())//
                .start(managerStep())//
                .build();
    }
}



@Configuration
class RabbitConfiguration {


    static final String REQUESTS = "requests";

    static final String REPLIES = "replies";

    @Bean
    Queue repliesQueue() {
        return QueueBuilder.durable(REPLIES).build();
    }

    @Bean
    Exchange repliesExchange() {
        return ExchangeBuilder.directExchange(REPLIES).durable(true).build();
    }

    @Bean
    Binding repliesBinding() {
        return BindingBuilder.bind(requestsQueue()).to(requestsExchange()).with(REPLIES).noargs();
    }

    @Bean
    Queue requestsQueue() {
        return QueueBuilder.durable(REQUESTS).build();
    }

    @Bean
    Exchange requestsExchange() {
        return ExchangeBuilder.directExchange(REQUESTS).durable(true).build();
    }

    @Bean
    Binding requestsBinding() {
        return BindingBuilder.bind(requestsQueue()).to(requestsExchange()).with(REQUESTS).noargs();
    }
}
