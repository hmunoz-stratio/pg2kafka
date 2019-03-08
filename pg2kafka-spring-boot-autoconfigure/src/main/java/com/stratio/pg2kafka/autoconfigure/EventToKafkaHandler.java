package com.stratio.pg2kafka.autoconfigure;

import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.integration.handler.GenericHandler;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

/**
 * Handler to send events to Kafka
 * 
 * @author hmunoz
 *
 */
public class EventToKafkaHandler extends IntegrationObjectSupport implements GenericHandler<Event> {

    private final KafkaTemplate<?, ?> kafkaTemplate;
    private final MessageToKafkaTransformer<?> transformer;

    public EventToKafkaHandler(KafkaTemplate<?, ?> kafkaTemplate, MessageToKafkaTransformer<?> transformer) {
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
        CustomAssert.isValid(payload, "'payload' must be not null and contains event data");
        CustomAssert.notNull(headers, "'headers' must not be null");
        Kafka.outboundChannelAdapter(kafkaTemplate)
                .sync(true)
                .topic(payload.getEventData().getTargetTopic())
                .messageKey(payload.getEventData().getTargetKey())
                .get()
                .handleMessage(
                        MessageBuilder.withPayload(transformer.transform(payload.getEventData().getMessage())).build());
        return payload;
    }

    private static class CustomAssert extends Assert {
        public static void isValid(Event event, String message) {
            notNull(event, message);
            notNull(event.getEventData(), message);
            notNull(event.getEventData().getTargetTopic(), message);
            notNull(event.getEventData().getTargetKey(), message);
            notNull(event.getEventData().getMessage(), message);
        }
    }
}
