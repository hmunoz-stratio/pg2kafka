package com.stratio.pg2kafka.autoconfigure;

import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

public class EventToKafkaHandler extends IntegrationObjectSupport implements GenericHandler<Event> {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final MessageToKafkaTransformer<?> transformer;

    public EventToKafkaHandler(KafkaTemplate<String, String> kafkaTemplate, MessageToKafkaTransformer<?> transformer) {
        Assert.notNull(transformer, "'transformer' must not be null");
        Assert.notNull(kafkaTemplate, "'kafkaTemplate' must not be null");
        this.transformer = transformer;
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
                .handleMessage(
                        MessageBuilder.withPayload(transformer.transform(payload.getEventData().getMessage())).build());
        return payload;
    }

}
