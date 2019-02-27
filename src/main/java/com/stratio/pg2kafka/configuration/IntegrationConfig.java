package com.stratio.pg2kafka.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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
import com.stratio.pg2kafka.Event;
import com.stratio.pg2kafka.components.EventCommandsHandler;
import com.stratio.pg2kafka.components.EventHeaders;
import com.stratio.pg2kafka.components.EventToKafkaHandler;
import com.stratio.pg2kafka.components.EventsListenerInboundChannelAdapter;

import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgPool;
import io.reactiverse.reactivex.pgclient.PgTransaction;
import io.vertx.core.VertxOptions;
import reactor.core.publisher.Mono;

@Configuration
@EnableIntegration
public class IntegrationConfig {

    @Autowired
    Pg2KafkaProperties applicationProperties;

    @Bean
    public EventCommandsHandler eventCommandsHandler() {
        return new EventCommandsHandler(applicationProperties.getEventsSource().getTableName());
    }

    @Bean
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
    public EventToKafkaHandler eventToKafkaHandler(KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper) {
        return new EventToKafkaHandler(kafkaTemplate, objectMapper);
    }

    @Bean
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
                .log(Level.DEBUG, IntegrationConfig.class.getName())
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
    public IntegrationFlow rollbackFlow() {
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
