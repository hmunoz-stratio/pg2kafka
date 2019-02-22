package com.stratio.pg2kafka.configuration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.pg2kafka.Event;
import com.stratio.pg2kafka.components.EventCommandsHandler;
import com.stratio.pg2kafka.components.EventForKafkaTransformer;
import com.stratio.pg2kafka.components.EventHeaders;
import com.stratio.pg2kafka.components.EventsListenerInboundChannelAdapter;

import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgPool;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@EnableIntegration
public class IntegrationConfig {

    @Autowired
    ApplicationProperties applicationProperties;

    @Bean
    public EventCommandsHandler eventCommandsHandler(JdbcTemplate jdbcTemplate) {
        return new EventCommandsHandler(jdbcTemplate, applicationProperties.getEventsSource().getTableName());
    }

    @Bean
    public MessageProducerSupport eventsListenerInboundChannelAdapter(PgPoolOptions pgPoolOptions, PgPool pgPool,
            ObjectMapper objectMapper) {
        EventsListenerInboundChannelAdapter adapter = new EventsListenerInboundChannelAdapter(pgPoolOptions, pgPool,
                applicationProperties.getEventsSource().getChannel(),
                applicationProperties.getEventsSource().getTableName(), objectMapper);
        adapter.setErrorChannelName("errorChannel");
        adapter.setOutputChannelName("eventChannel");
        return adapter;
    }

    @Bean
    public IntegrationFlow eventsListenerFlow(EventCommandsHandler eventCommandsHandler,
            KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        return IntegrationFlows
                .from("eventChannel")
                .transform(Message.class, m -> {
                    MessageHeaders mutableHeaders = new MutableMessageHeaders(m.getHeaders());
                    Event payload = (Event) m.getPayload();
                    mutableHeaders.put(EventHeaders.EVENT_ID_HEADER, payload.getEventData().getId());
                    return MessageBuilder.createMessage(payload, mutableHeaders);
                })
                .log(Level.DEBUG, IntegrationConfig.class.getName())
                .handle(Event.class,
                        (p, h) -> {
                            long eventId = h.get(EventHeaders.EVENT_ID_HEADER, Long.class);
                            if (eventCommandsHandler.tryAdquire(eventId)) {
                                return p;
                            }
                            log.debug("Discarted message for event id: {}. Other instance should be processing it.",
                                    eventId);
                            return null;
                        },
                        e -> e.transactional(true))
                .transform(new EventForKafkaTransformer(objectMapper)).handle((p, h) -> {
                    Message<?> m = MessageBuilder.createMessage(p, h);
                    Kafka.outboundChannelAdapter(kafkaTemplate).sync(true).get().handleMessage(m);
                    return m;
                })
                .handle(m -> eventCommandsHandler
                        .markProcessed(m.getHeaders().get(EventHeaders.EVENT_ID_HEADER, Long.class)))
                .get();
    }

}
