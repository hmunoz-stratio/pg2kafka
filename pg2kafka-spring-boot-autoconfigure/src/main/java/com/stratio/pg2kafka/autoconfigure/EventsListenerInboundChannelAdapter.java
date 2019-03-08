package com.stratio.pg2kafka.autoconfigure;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.commons.lang3.StringUtils;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.stratio.pg2kafka.autoconfigure.Event.EventData;

import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.reactivex.pgclient.PgPool;
import io.reactiverse.reactivex.pgclient.PgStream;
import io.reactiverse.reactivex.pgclient.Row;
import io.reactiverse.reactivex.pgclient.Tuple;
import io.reactiverse.reactivex.pgclient.pubsub.PgChannel;
import io.reactiverse.reactivex.pgclient.pubsub.PgSubscriber;
import io.reactivex.Observable;
import io.vertx.core.Handler;
import io.vertx.core.VertxOptions;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventsListenerInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable {

    private static final int DEFAULT_FETCH_SIZE = 100;

    private final VertxOptions vertxOptions;
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

    public EventsListenerInboundChannelAdapter(VertxOptions vertxOptions, PgPoolOptions pgPoolOptions, PgPool pgPool, String channel,
            String tableName) {
        this(vertxOptions, pgPoolOptions, pgPool, channel, tableName, null);
    }

    public EventsListenerInboundChannelAdapter(VertxOptions vertxOptions, PgPoolOptions pgPoolOptions, PgPool pgPool, String channel,
            String tableName, ObjectMapper objectMapper) {
        Assert.notNull(vertxOptions, "'vertxOptions' must not be null");
        Assert.notNull(pgPoolOptions, "'pgPoolOptions' must not be null");
        Assert.notNull(pgPool, "'pgPool' must not be null");
        Assert.hasText(channel, "'channel' must not be null");
        Assert.hasText(tableName, "'tableName' must not be null");
        this.vertxOptions = vertxOptions;
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
                .subscriber(Vertx.vertx(vertxOptions), pgPoolOptions)
                .connect(a -> {
                })
                .reconnectPolicy(r -> 0l);
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

        @Override
        public void handle(String payload) {
            if (!ready) {
                throw new IllegalStateException("Handler must be started before handle");
            }
            Observable<Row> pendingsObservable = pendingsObservable();
            pendingsObservable.blockingSubscribe(
                    r -> sendMessage(MessageBuilder.withPayload(toEvent(r)).build()),
                    e -> log.error(e.getMessage(), e));
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
                return Event.builder()
                        .schema(schemaName)
                        .table(tableName)
                        .action("INSERT")
                        .eventData(objectMapper.readValue(row.getString(0), EventData.class))
                        .build();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

}
