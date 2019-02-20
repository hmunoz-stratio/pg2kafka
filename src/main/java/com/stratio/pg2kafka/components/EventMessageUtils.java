package com.stratio.pg2kafka.components;

import java.io.IOException;
import java.util.Objects;

import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import wiremock.com.google.common.util.concurrent.UncheckedExecutionException;

@Component
public class EventMessageUtils {

    public static final String EVENT_ID_HEADER = "internal.eventId";

    private final ObjectMapper objectMapper;

    public EventMessageUtils() {
        this(null);
    }

    public EventMessageUtils(ObjectMapper objectMapper) {
        this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
    }

    public JsonNode toJsonNode(Object payloadObj) {
        try {
            String payload = Objects.toString(payloadObj, "{}");
            return objectMapper.readValue(payload, JsonNode.class);
        } catch (IOException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    public long extractEventId(Object payloadObj) {
        return toJsonNode(payloadObj).get("data").get("id").asLong();
    }

    public String serialize(JsonNode jsonNode) {
        try {
            return objectMapper.writeValueAsString(jsonNode);
        } catch (IOException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    public Message<Object> enrichEventIdHeader(Message<?> message) {
        MessageHeaders mutableHeaders = new MutableMessageHeaders(message.getHeaders());
        Object payload = message.getPayload();
        mutableHeaders.put(EVENT_ID_HEADER, extractEventId(payload));
        return MessageBuilder.createMessage(payload, mutableHeaders);
    }

}
