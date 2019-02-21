package com.stratio.pg2kafka.components;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgPool;
import io.reactiverse.reactivex.pgclient.PgStream;
import io.reactiverse.reactivex.pgclient.Row;
import io.reactiverse.reactivex.pgclient.Tuple;
import io.reactiverse.reactivex.pgclient.pubsub.PgChannel;
import io.reactiverse.reactivex.pgclient.pubsub.PgSubscriber;
import io.reactivex.Observable;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventsListenerInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {

    private final PgPoolOptions pgPoolOptions;
    private final PgPool pgPool;
    private final String channel;
    private final String schemaName;
    private final String tableName;
    private final EventMessageUtils eventMessageUtils;
    private final ObjectMapper objectMapper;
    private PgSubscriber pgSubscriber;
    private PgChannel pgChannel;

    public EventsListenerInboundChannelAdapter(PgPoolOptions pgPoolOptions, PgPool pgPool, String channel,
            String tableName, EventMessageUtils eventMessageUtils) {
        this(pgPoolOptions, pgPool, channel, tableName, eventMessageUtils, null);
    }

    public EventsListenerInboundChannelAdapter(PgPoolOptions pgPoolOptions, PgPool pgPool, String channel,
            String tableName, EventMessageUtils eventMessageUtils, ObjectMapper objectMapper) {
        Assert.notNull(pgPoolOptions, "'pgPoolOptions' must not be null");
        Assert.notNull(pgPool, "'pgPool' must not be null");
        Assert.hasText(channel, "'channel' must not be null");
        Assert.hasText(tableName, "'tableName' must not be null");
        Assert.notNull(eventMessageUtils, "'eventMessageUtils' must not be null");
        this.pgPoolOptions = pgPoolOptions;
        this.pgPool = pgPool;
        this.channel = channel;
        this.schemaName = StringUtils.contains(tableName, ".") ? StringUtils.substringBefore(tableName, ".") : "public";
        this.tableName = StringUtils.substringAfter(tableName, ".");
        this.eventMessageUtils = eventMessageUtils;
        this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
    }

    @Override
    public String getComponentType() {
        return "eventslistener:inbound-channel-adapter";
    }

    @Override
    protected void onInit() {
        pgSubscriber = PgSubscriber.subscriber(Vertx.vertx(), pgPoolOptions).connect(a -> {
        });
        pgChannel = pgSubscriber.channel(channel);

        super.onInit();
    }

    @Override
    protected void doStart() {
        Observable<Row> observable = pendingsObservable();
        observable.blockingSubscribe(
                r -> sendMessage(MessageBuilder.withPayload(toPayload(r)).build()),
                e -> log.error(e.getMessage(), e),
                this::initChannelListen);
        super.doStart();
    }

    @Override
    protected void doStop() {
        if (channel != null) {
            pgChannel.pause();
        }
        super.doStop();
    }

    @Override
    public int beforeShutdown() {
        if (pgSubscriber != null) {
            pgSubscriber.close();
        }
        return 0;
    }

    @Override
    public int afterShutdown() {
        return 0;
    }

    private String toPayload(Row row) throws IOException {
        Map<String, Object> dataMap = objectMapper.readValue(row.getString(0),
                new TypeReference<Map<String, Object>>() {
                });
        return objectMapper.writeValueAsString(
                Collections.unmodifiableMap(Stream.<Map.Entry<String, Object>>of(
                        entry("schema", schemaName),
                        entry("table", tableName),
                        entry("action", "INSERT"),
                        entry("data", dataMap))
                        .collect(entriesToMap())));
    }

    private <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    private <K, U> Collector<Map.Entry<K, U>, ?, Map<K, U>> entriesToMap() {
        return Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue);
    }

    private Observable<Row> pendingsObservable() {
        return pgPool.rxBegin()
                .flatMapObservable(tx -> tx
                        .rxPrepare("SELECT row_to_json(t)::text FROM " + schemaName + "." + tableName
                                + " AS t WHERE t.process_date IS NULL ORDER BY t.id asc")
                        .flatMapObservable(preparedQuery -> {
                            PgStream<Row> stream = preparedQuery.createStream(50, Tuple.tuple());
                            return stream.toObservable();
                        })
                        .doAfterTerminate(tx::commit));
    }

    private void initChannelListen() {
        pgChannel.handler(p -> {
            long eventId = eventMessageUtils.extractEventId(p);
            log.debug("eventId: {}", eventId);
            Observable<Row> observable = eventObservable(eventId);
            observable.blockingSubscribe(
                    r -> {
                        String payload = toPayload(r);
                        log.debug("Payload: {}", payload);
                        sendMessage(MessageBuilder.withPayload(payload).setCorrelationId(eventId).build());
                    },
                    e -> log.error(e.getMessage(), e));
        });
    }

    private Observable<Row> eventObservable(long eventId) {
        return pgPool.rxBegin()
                .flatMapObservable(tx -> tx
                        .rxPrepare("SELECT row_to_json(t)::text FROM " + schemaName + "." + tableName
                                + " AS t WHERE t.id <= $1 AND t.process_date IS NULL ORDER BY t.id asc")
                        .flatMapObservable(preparedQuery -> {
                            PgStream<Row> stream = preparedQuery.createStream(50,
                                    Tuple.of(eventId));
                            return stream.toObservable();
                        })
                        .doAfterTerminate(tx::commit));
    }
}
