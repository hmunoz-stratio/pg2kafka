package com.stratio.pg2kafka.integration.configuration;

import java.util.Objects;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.KafkaTemplate;

import com.stratio.pg2kafka.integration.inbound.PostgresListenerInboundChannelAdapter;

import io.reactiverse.reactivex.pgclient.pubsub.PgChannel;
import io.reactiverse.reactivex.pgclient.pubsub.PgSubscriber;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class PostgresListenerIntegrationConfig {

    @Bean
    public PgChannel pgEventChannel(PgSubscriber pgSubscriber) {
        return pgSubscriber.channel("events");
    }

    @Bean
    public IntegrationFlow inboundChannelAdapterFlow(PgChannel pgEventChannel,
            KafkaTemplate<String, String> kafkaTemplate) {
        return IntegrationFlows
                .from(new PostgresListenerInboundChannelAdapter(pgEventChannel))
                .log(Level.INFO)
                .handle(Kafka.outboundChannelAdapter(kafkaTemplate).topic("test"))
                .get();
    }

}
