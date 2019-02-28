package com.stratio.pg2kafka.autoconfigure;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.ErrorMessage;
import org.springframework.messaging.support.MessageBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgClient;
import io.reactiverse.reactivex.pgclient.PgPool;
import io.reactiverse.reactivex.pgclient.PgTransaction;
import io.vertx.core.VertxOptions;
import io.vertx.reactivex.core.Vertx;
import reactor.core.publisher.Mono;

@Configuration
@EnableConfigurationProperties(Pg2KafkaProperties.class)
@EnableIntegration
@Import(KafkaAutoConfiguration.class)
public class Pg2KafkaAutoConfiguration {

    @Autowired
    Pg2KafkaProperties applicationProperties;

    @Bean
    @ConditionalOnMissingBean
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    @Bean
    @ConditionalOnMissingBean
    public PgPoolOptions postgresClientPoolOptions(Pg2KafkaProperties pg2KafkaProperties) {
        return PgPoolOptions
                .fromUri(pg2KafkaProperties.getEventsSource().getUrl())
                .setUser(pg2KafkaProperties.getEventsSource().getUsername())
                .setPassword(pg2KafkaProperties.getEventsSource().getPassword());
    }

    @Bean
    @ConditionalOnMissingBean
    public VertxOptions vertxOptions() {
        VertxOptions options = new VertxOptions();
        options.setMaxEventLoopExecuteTime(10L * 1000 * 1000000); // 10 seconds
        return options;
    }

    @Bean
    @ConditionalOnMissingBean
    public PgPool postgresClientPool(VertxOptions vertxOptions, PgPoolOptions pgPoolOptions) {
        return PgClient.pool(Vertx.vertx(vertxOptions), pgPoolOptions);
    }

    @Bean
    @ConditionalOnMissingBean
    public EventCommandsHandler eventCommandsHandler() {
        return new EventCommandsHandler(applicationProperties.getEventsSource().getTableName());
    }

    @Bean
    @ConditionalOnMissingBean
    public EventsListenerInboundChannelAdapter eventsListenerInboundChannelAdapter(VertxOptions vertxOptions,
            PgPoolOptions pgPoolOptions, PgPool pgPool, ObjectMapper objectMapper) {
        EventsListenerInboundChannelAdapter adapter = new EventsListenerInboundChannelAdapter(vertxOptions,
                pgPoolOptions, pgPool, applicationProperties.getEventsSource().getChannel(),
                applicationProperties.getEventsSource().getTableName(), objectMapper);
        adapter.setErrorChannelName("errorChannel");
        adapter.setOutputChannelName("eventChannel");
        return adapter;
    }

    @Bean
    @ConditionalOnMissingBean
    public EventToKafkaHandler eventToKafkaHandler(KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        return new EventToKafkaHandler(kafkaTemplate, objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public IntegrationFlow eventsListenerFlow(EventCommandsHandler eventCommandsHandler,
            EventToKafkaHandler eventToKafkaHandler, PgPool pgPool) {
        return IntegrationFlows
                .from("eventChannel")
                .transform(Message.class, m -> {
                    MessageHeaders mutableHeaders = new MutableMessageHeaders(m.getHeaders());
                    Event payload = (Event) m.getPayload();
                    mutableHeaders.put(EventHeaders.EVENT_ID_HEADER, payload.getEventData().getId());
                    mutableHeaders.put(EventHeaders.EVENT_TX, pgPool.rxBegin().blockingGet());
                    return MessageBuilder.createMessage(payload, mutableHeaders);
                })
                .log(Level.DEBUG, Pg2KafkaAutoConfiguration.class.getName())
                .handle(Event.class,
                        (p, h) -> {
                            final long eventId = h.get(EventHeaders.EVENT_ID_HEADER, Long.class);
                            PgTransaction tx = (PgTransaction) h.get(EventHeaders.EVENT_TX);
                            Mono<Object> mono = eventCommandsHandler.tryAcquire(tx, eventId)
                                    .flatMap(b -> {
                                        if (b) {
                                            return Mono.just(p);
                                        }
                                        tx.commit();
                                        return Mono.empty();
                                    });
                            return mono.block();
                        })

                .handle(eventToKafkaHandler)
                .handle(Event.class, (p, h) -> {
                    final long eventId = h.get(EventHeaders.EVENT_ID_HEADER, Long.class);
                    PgTransaction tx = (PgTransaction) h.get(EventHeaders.EVENT_TX);
                    Mono<Event> mono = eventCommandsHandler.markProcessed(tx, eventId)
                            .thenReturn(p);
                    return mono.block();
                })
                .handle(m -> {
                    PgTransaction tx = (PgTransaction) m.getHeaders().get(EventHeaders.EVENT_TX);
                    tx.commit();
                })
                .get();
    }

    @Bean
    @ConditionalOnMissingBean
    public IntegrationFlow eventRollbackFlow() {
        return IntegrationFlows
                .from("errorChannel")
                .handle(ErrorMessage.class, (p, h) -> {
                    Throwable throwable = p.getPayload();
                    if (throwable instanceof MessagingException) {
                        MessagingException messagingException = (MessagingException) throwable;
                        Message<?> failedMessage = messagingException.getFailedMessage();
                        if (failedMessage != null) {
                            PgTransaction tx = (PgTransaction) failedMessage.getHeaders().get(EventHeaders.EVENT_TX);
                            if (tx != null) {
                                tx.rollback();
                            }
                        }
                    }
                    return null;
                })
                .get();
    }

}
