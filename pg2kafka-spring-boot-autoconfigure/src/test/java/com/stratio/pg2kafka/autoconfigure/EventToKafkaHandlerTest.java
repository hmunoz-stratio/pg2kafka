package com.stratio.pg2kafka.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.springframework.kafka.test.assertj.KafkaConditions.key;
import static org.springframework.kafka.test.assertj.KafkaConditions.value;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.junit4.SpringRunner;

import com.stratio.pg2kafka.autoconfigure.Event.EventData;

@RunWith(SpringRunner.class)
//@DirtiesContext
@EmbeddedKafka(topics = EventToKafkaHandlerTest.TEST_TOPIC, controlledShutdown = true)
public class EventToKafkaHandlerTest {

//    static {
//        System.setProperty(EmbeddedKafkaBroker.BROKER_LIST_PROPERTY,
//                "spring.kafka.bootstrap-servers");
//    }

    static final String TEST_TOPIC = "aTargetTopic";

    static final Event aValidEvent = Event.builder()
            .action("INSERT")
            .eventData(EventData.builder()
                    .creationDate(OffsetDateTime.now())
                    .id(1l)
                    .message(Collections.singletonMap("raw", "{\"key\":\"value\"}"))
                    .targetKey("aTargetKey")
                    .targetTopic(TEST_TOPIC)
                    .build())
            .schema("aSchema")
            .table("anEventTable")
            .build();

    EventToKafkaHandler classUnderTest;
    KafkaMessageListenerContainer<String, Object> container;
    final BlockingQueue<ConsumerRecord<String, Object>> records = new LinkedBlockingQueue<>();
    KafkaTemplate<?, ?> kafkaTemplate;

    @Autowired
    EmbeddedKafkaBroker kafkaBroker;

    @Before
    public void setUp() throws Exception {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", kafkaBroker);
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer");
        DefaultKafkaConsumerFactory<String, Object> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        ContainerProperties containerProperties = new ContainerProperties(TEST_TOPIC);
        container = new KafkaMessageListenerContainer<>(cf, containerProperties);
//        final BlockingQueue<ConsumerRecord<Integer, String>> records = new LinkedBlockingQueue<>();
        container.setupMessageListener(new MessageListener<String, Object>() {
            @Override
            public void onMessage(ConsumerRecord<String, Object> record) {
                System.out.println("Consumed record: " + record);
                records.add(record);
            }

        });
//        container.setBeanName("templateTests");
        container.start();
        ContainerTestUtils.waitForAssignment(container, kafkaBroker.getPartitionsPerTopic());
        Map<String, Object> senderProps = KafkaTestUtils.senderProps(kafkaBroker.getBrokersAsString());
        senderProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        senderProps.put("value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer");
        ProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<String, String>(senderProps);
        kafkaTemplate = new KafkaTemplate<>(pf);
    }

    @After
    public void tearDown() throws Exception {
        container.stop();
    }

    @Test
    public void givenNullKafkaTemplate_whenConstruct_thenIllegalArgumentExceptionIsThrown() {
        assertThatThrownBy(() -> classUnderTest = new EventToKafkaHandler(null, s -> s))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void givenNullTransformer_whenConstruct_thenIllegalArgumentExceptionIsThrown() {
        assertThatThrownBy(() -> classUnderTest = new EventToKafkaHandler(kafkaTemplate, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void givenNullPayload_whenHandle_thenIllegalArgumentExceptionIsThrown() {
        classUnderTest = new EventToKafkaHandler(kafkaTemplate, s -> s);
        assertThatThrownBy(() -> classUnderTest.handle(null, new MessageHeaders(null)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void givenAPayloadWithoutEventData_whenHandle_thenIllegalArgumentExceptionIsThrown() {
        classUnderTest = new EventToKafkaHandler(kafkaTemplate, s -> s);

        assertThatThrownBy(() -> classUnderTest.handle(Event.builder().build(), new MessageHeaders(null)))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> classUnderTest.handle(Event.builder()
                .eventData(EventData.builder().build())
                .build(), new MessageHeaders(null)))
                        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void givenNullHeaders_whenHandle_thenIllegalArgumentExceptionIsThrown() {
        classUnderTest = new EventToKafkaHandler(kafkaTemplate, s -> s);
        assertThatThrownBy(() -> classUnderTest.handle(aValidEvent, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void givenAPayload_whenHandle_thenKafkaMessageMustBeSentToEventTopic() throws InterruptedException {
        classUnderTest = new EventToKafkaHandler(kafkaTemplate, s -> s);

        classUnderTest.handle(aValidEvent, new MessageHeaders(null));

        ConsumerRecord<String, Object> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received).hasFieldOrPropertyWithValue("topic", TEST_TOPIC);
    }

    @Test
    public void givenAPayload_whenHandle_thenKafkaMessageMustBeSentToEventKey() throws InterruptedException {
        classUnderTest = new EventToKafkaHandler(kafkaTemplate, s -> s);

        classUnderTest.handle(aValidEvent, new MessageHeaders(null));

        ConsumerRecord<String, Object> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received).has(key(aValidEvent.getEventData().getTargetKey()));
    }

    @Test
    public void givenAPayload_whenHandle_thenKafkaMessageValueIsTheEventDataMessage() throws InterruptedException {
        classUnderTest = new EventToKafkaHandler(kafkaTemplate, s -> s);

        classUnderTest.handle(aValidEvent, new MessageHeaders(null));

        ConsumerRecord<String, Object> received = records.poll(10, TimeUnit.SECONDS);
        assertThat(received).has(value(aValidEvent.getEventData().getMessage()));
    }

    @Test
    public void givenAPayload_whenHandle_thenTransformerMustBeInvoked() throws InterruptedException {
        MessageToKafkaTransformer<Object> transformer = new MessageToKafkaTransformer<Object>() {
            @Override
            public Object transform(Map<String, Object> input) {
                return input;
            }
        };
        MessageToKafkaTransformer<Object> transformerSpy = spy(transformer);
        classUnderTest = new EventToKafkaHandler(kafkaTemplate, transformerSpy);

        classUnderTest.handle(aValidEvent, new MessageHeaders(null));

        verify(transformerSpy).transform(eq(aValidEvent.getEventData().getMessage()));
    }

}
