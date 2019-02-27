package com.stratio.pg2kafka.components;

import java.io.UncheckedIOException;
import java.util.Map;

import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.pg2kafka.Event;

public class EventToKafkaHandler extends IntegrationObjectSupport implements GenericHandler<Event> {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public EventToKafkaHandler(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        Assert.notNull(kafkaTemplate, "'kafkaTemplate' must not be null");
        this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public String getComponentType() {
        return "eventToKafka:handler";
    }

    @Override
    public Object handle(Event payload, MessageHeaders headers) {
        Kafka.outboundChannelAdapter(kafkaTemplate)
                .sync(true)
                .topic(payload.getEventData().getTargetTopic())
                .messageKey(payload.getEventData().getTargetKey())
                .get()
                .handleMessage(MessageBuilder.withPayload(serialize(payload.getEventData().getMessage())).build());
        return payload;
    }

    private String serialize(Map<String, Object> message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

}
