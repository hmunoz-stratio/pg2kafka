package com.stratio.pg2kafka.components;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.pg2kafka.Event;
import com.stratio.pg2kafka.Event.EventData;

import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgPool;
import io.reactiverse.reactivex.pgclient.PgStream;
import io.reactiverse.reactivex.pgclient.Row;
import io.reactiverse.reactivex.pgclient.Tuple;
import io.reactiverse.reactivex.pgclient.pubsub.PgChannel;
import io.reactiverse.reactivex.pgclient.pubsub.PgSubscriber;
import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventsListenerInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {

    private static final int DEFAULT_FETCH_SIZE = 100;

    private final PgPoolOptions pgPoolOptions;
    private final PgPool pgPool;
    private final String channel;
    private final String schemaName;
    private final String tableName;
    private final ObjectMapper objectMapper;
    private int fetchSize = DEFAULT_FETCH_SIZE;
    private PgSubscriber pgSubscriber;
    private PgChannel pgChannel;
    private EventHandler eventHandler;

    public EventsListenerInboundChannelAdapter(PgPoolOptions pgPoolOptions, PgPool pgPool, String channel,
            String tableName) {
        this(pgPoolOptions, pgPool, channel, tableName, null);
    }

    public EventsListenerInboundChannelAdapter(PgPoolOptions pgPoolOptions, PgPool pgPool, String channel,
            String tableName, ObjectMapper objectMapper) {
        Assert.notNull(pgPoolOptions, "'pgPoolOptions' must not be null");
        Assert.notNull(pgPool, "'pgPool' must not be null");
        Assert.hasText(channel, "'channel' must not be null");
        Assert.hasText(tableName, "'tableName' must not be null");
        this.pgPoolOptions = pgPoolOptions;
        this.pgPool = pgPool;
        this.channel = channel;
        this.schemaName = StringUtils.contains(tableName, ".") ? StringUtils.substringBefore(tableName, ".") : "public";
        this.tableName = StringUtils.substringAfter(tableName, ".");
        this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
    }

    public void setFetchSize(int fetchSize) {
        Assert.isTrue(fetchSize > 0, "'fetchSize' must be positive");
        this.fetchSize = fetchSize;
    }

    @Override
    public String getComponentType() {
        return "eventslistener:inbound-channel-adapter";
    }

    @Override
    protected void onInit() {
        pgSubscriber = PgSubscriber
                .subscriber(Vertx.vertx(), pgPoolOptions)
                .connect(a -> {
                })
                .reconnectPolicy(r -> {
                    if (eventHandler != null) {
                        eventHandler.markDesynchronized();
                    }
                    return 0l;
                });
        pgChannel = pgSubscriber.channel(channel);
        eventHandler = new EventHandler();
        super.onInit();
    }

    @Override
    protected void doStart() {
        eventHandler.start();
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
        return getPhase();
    }

    @Override
    public int afterShutdown() {
        return getPhase();
    }

    class EventHandler implements Handler<String> {

        private volatile boolean ready = false;
        private volatile boolean isSynchronized = false;

        public void start() {
            log.debug("Pendings processing start");
            Observable<Row> pendingsObservable = pendingsObservable();
            pendingsObservable.blockingSubscribe(
                    r -> sendMessage(MessageBuilder.withPayload(toEvent(r)).build()),
                    e -> log.error(e.getMessage(), e),
                    () -> pgChannel.handler(this));
            ready = true;
            log.debug("Pendings processing finished");
        }

        public void markDesynchronized() {
            isSynchronized = false;
        }

        @Override
        public void handle(String payload) {
            if (!ready) {
                throw new IllegalStateException("Handler must be started before handle");
            }
            Event event = toEvent(payload);
            if (isSynchronized) {
                sendMessage(MessageBuilder.withPayload(event).build());
            } else {
                log.debug("Handling with desynchronized behavior");
                long eventId = event.getEventData().getId();
                pgPool.rxBegin()
                        .flatMapObservable(tx -> tx
                                .rxPrepare("SELECT row_to_json(t)::text FROM " + schemaName + "." + tableName
                                        + " AS t WHERE t.id <= $1 AND t.process_date IS NULL ORDER BY t.id asc")
                                .flatMapObservable(preparedQuery -> {
                                    PgStream<Row> stream = preparedQuery.createStream(fetchSize, Tuple.of(eventId));
                                    return stream.toObservable();
                                })
                                .doAfterTerminate(tx::commit))
                        .subscribe(
                                p -> sendMessage(MessageBuilder.withPayload(toEvent(p)).build()),
                                e -> log.error(e.getMessage(), e));
                isSynchronized = true;
            }
        }

        private Observable<Row> pendingsObservable() {
            return pgPool.rxBegin()
                    .flatMapObservable(tx -> tx
                            .rxPrepare("SELECT row_to_json(t)::text FROM " + schemaName + "." + tableName
                                    + " AS t WHERE t.process_date IS NULL ORDER BY t.id asc")
                            .flatMapObservable(preparedQuery -> {
                                PgStream<Row> stream = preparedQuery.createStream(fetchSize, Tuple.tuple());
                                return stream.toObservable();
                            })
                            .doAfterTerminate(tx::commit));
        }

        private Event toEvent(Row row) {
            try {
                Event event = new Event();
                event.setSchema(schemaName);
                event.setTable(tableName);
                event.setAction("INSERT");
                event.setEventData(objectMapper.readValue(row.getString(0), EventData.class));
                return event;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private Event toEvent(String payload) {
            try {
                return objectMapper.readValue(payload, Event.class);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

}
