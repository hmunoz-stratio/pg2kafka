package com.stratio.pg2kafka.components;

import java.util.Map;

import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.pg2kafka.Event;

public class EventForKafkaTransformer extends AbstractTransformer {

    private final ObjectMapper objectMapper;

    public EventForKafkaTransformer(ObjectMapper objectMapper) {
        this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
    }

    @Override
    public String getComponentType() {
        return "eventForKafka:transformer";
    }

    @Override
    protected Object doTransform(Message<?> message) throws Exception {
        Event event = (Event) message.getPayload();
        MessageHeaders headers = new MutableMessageHeaders(message.getHeaders());
        headers.put(KafkaHeaders.TOPIC, event.getEventData().getTargetTopic());
        headers.put(KafkaHeaders.MESSAGE_KEY, event.getEventData().getTargetKey());
        return MessageBuilder.createMessage(serialize(event.getEventData().getMessage()), headers);
    }

    private String serialize(Map<String, Object> message) throws JsonProcessingException {
        return objectMapper.writeValueAsString(message);
    }

}
