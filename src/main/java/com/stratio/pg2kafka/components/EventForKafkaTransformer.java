package com.stratio.pg2kafka.components;

import org.springframework.integration.support.MutableMessageHeaders;
import org.springframework.integration.transformer.AbstractTransformer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.JsonNode;

public class EventForKafkaTransformer extends AbstractTransformer {

    private final EventMessageUtils eventMessageUtils;

    public EventForKafkaTransformer(EventMessageUtils eventMessageUtils) {
        Assert.notNull(eventMessageUtils, "'eventMessageUtils' must be not null");
        this.eventMessageUtils = eventMessageUtils;
    }

    @Override
    public String getComponentType() {
        return "eventForKafka:transformer";
    }

    @Override
    protected Object doTransform(Message<?> message) throws Exception {
        JsonNode json = eventMessageUtils.toJsonNode(message.getPayload());
        MessageHeaders headers = new MutableMessageHeaders(message.getHeaders());
        headers.put(KafkaHeaders.TOPIC, extractTopic(json));
        headers.put(KafkaHeaders.MESSAGE_KEY, extractKey(json));
        return MessageBuilder.createMessage(extractData(json), headers);
    }

    private String extractTopic(JsonNode json) {
        return json.get("data").get("target_topic").textValue();
    }

    private String extractKey(JsonNode json) {
        return json.get("data").get("target_key").textValue();
    }

    private String extractData(JsonNode json) {
        return eventMessageUtils.serialize(json.get("data").get("data"));
    }

}
