package com.stratio.pg2kafka.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.integration.annotation.Router;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.handler.LoggingHandler.Level;
import org.springframework.integration.kafka.dsl.Kafka;
import org.springframework.integration.router.ErrorMessageExceptionTypeRouter;
import org.springframework.integration.router.MessageRouter;
import org.springframework.integration.transformer.Transformer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import com.stratio.pg2kafka.components.EventCommandsHandler;
import com.stratio.pg2kafka.components.EventForKafkaTransformer;
import com.stratio.pg2kafka.components.EventMessageUtils;
import com.stratio.pg2kafka.components.EventsListenerInboundChannelAdapter;

import io.reactiverse.reactivex.pgclient.pubsub.PgChannel;
import io.reactiverse.reactivex.pgclient.pubsub.PgSubscriber;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class PostgresListenerIntegrationConfig {

    public static final String IGNORE_CHANNEL_BEAN_NAME = "ignoreChannel";

    @Bean
    public PgChannel pgEventChannel(PgSubscriber pgSubscriber) {
        return pgSubscriber.channel("events");
    }

    @Bean
    public Transformer eventForKafkaTransformer(EventMessageUtils eventMessageUtils) {
        return new EventForKafkaTransformer(eventMessageUtils);
    }

    @Bean
    public EventCommandsHandler eventCommandsHandler(JdbcTemplate jdbcTemplate,
            @Value("${events.table-name}") String tableName) {
        return new EventCommandsHandler(jdbcTemplate, tableName);
    }

    @Bean
    public MessageProducerSupport eventsListenerInboundChannelAdapter(PgChannel pgEventChannel) {
        EventsListenerInboundChannelAdapter adapter = new EventsListenerInboundChannelAdapter(pgEventChannel);
        adapter.setErrorChannelName("errorRoutingChannel");
        return adapter;
    }

    @Bean
    public IntegrationFlow eventsListenerFlow(MessageProducerSupport messageProducerSupport,
            EventCommandsHandler eventCommandsHandler, KafkaTemplate<String, String> kafkaTemplate,
            Transformer eventForKafkaTransformer, EventMessageUtils eventMessageUtils) {
        return IntegrationFlows
                .from(messageProducerSupport)
                .log(Level.INFO)
                .transform(Message.class, eventMessageUtils::enrichEventIdHeader)
                .handle(Object.class, (p, h) -> {
                    eventCommandsHandler.tryLock(h.get(EventMessageUtils.EVENT_ID_HEADER, Long.class));
                    return p;
                }, e -> e.transactional(true))
                .transform(eventForKafkaTransformer)
                .handle((p, h) -> {
                    Message<?> m = MessageBuilder.createMessage(p, h);
                    Kafka.outboundChannelAdapter(kafkaTemplate).get().handleMessage(m);
                    return m;
                })
                .handle(m -> eventCommandsHandler
                        .markProcessed(m.getHeaders().get(EventMessageUtils.EVENT_ID_HEADER, Long.class)))
                .get();
    }

    @Bean
    @Router(inputChannel = "errorRoutingChannel")
    public MessageRouter errorRouter() {
        ErrorMessageExceptionTypeRouter router = new ErrorMessageExceptionTypeRouter();
        router.setChannelMapping(CannotAcquireLockException.class.getName(), IGNORE_CHANNEL_BEAN_NAME);
        router.setChannelMapping(EmptyResultDataAccessException.class.getName(), IGNORE_CHANNEL_BEAN_NAME);
        router.setDefaultOutputChannelName(IntegrationContextUtils.ERROR_CHANNEL_BEAN_NAME);
        return router;
    }

    @Bean
    public IntegrationFlow ignoreFlow() {
        return IntegrationFlows
                .from(IGNORE_CHANNEL_BEAN_NAME)
                .log(Level.INFO)
                .get();
    }

}
