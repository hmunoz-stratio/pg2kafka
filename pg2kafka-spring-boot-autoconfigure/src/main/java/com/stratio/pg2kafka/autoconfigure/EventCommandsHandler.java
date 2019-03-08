package com.stratio.pg2kafka.autoconfigure;

import java.util.function.Predicate;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.integration.context.IntegrationObjectSupport;
import org.springframework.util.Assert;

import io.reactiverse.pgclient.PgException;
import io.reactiverse.reactivex.pgclient.PgTransaction;
import io.reactiverse.reactivex.pgclient.Tuple;
import lombok.extern.slf4j.Slf4j;
import reactor.adapter.rxjava.RxJava2Adapter;
import reactor.core.publisher.Mono;

/**
 * Event commands handler
 * 
 * @author hmunoz
 *
 */
@Slf4j
public class EventCommandsHandler extends IntegrationObjectSupport {

    private final String tableName;

    public EventCommandsHandler(String tableName) {
        Assert.hasText(tableName, "'tableName' must not be empty");
        this.tableName = tableName;
    }

    @Override
    public String getComponentType() {
        return "eventCommands:handler";
    }

    /**
     * Tries to acquire the event for the input eventId
     * 
     * @param tx      the transaction
     * @param eventId the event id
     * @return a {@link Mono} that emits true is the acquire is successful, false
     *         otherwise
     * @throws IllegalArgumentException when the input tx is null
     */
    public Mono<Boolean> tryAcquire(PgTransaction tx, long eventId) {
        Assert.notNull(tx, "'tx' must not be null");
        log.debug("Trying to acquire the event with id: {}", eventId);
        return Mono.from(tx.rxPreparedQuery(
                "SELECT id FROM " + tableName + " WHERE id = $1 AND process_date IS NULL FOR UPDATE NOWAIT",
                Tuple.of(eventId))
                .toFlowable())
                .map(r -> r.rowCount() > 0)
                .onErrorReturn(couldNotLockPredicate, false)
                .doOnSuccess(b -> log.debug("Event acquisition returns '{}' for id: {}", b, eventId));
    }

    /**
     * Marks the event for the input eventId as processed
     * 
     * @param tx      the transaction
     * @param eventId the event id
     * @return a {@link Mono} that completes without value
     * @throws IllegalArgumentException when the input tx is null
     */
    public Mono<Void> markProcessed(PgTransaction tx, long eventId) {
        Assert.notNull(tx, "'tx' must not be null");
        log.debug("Marking as processed event with id: {}", eventId);
        return RxJava2Adapter.completableToMono(tx.rxPreparedQuery(
                "UPDATE " + tableName + " SET process_date = now() WHERE id = $1 AND process_date IS NULL",
                Tuple.of(eventId))
                .ignoreElement())
                .doOnSuccess(r -> log.debug("Marked as processed event with id: {}", eventId));
    }

    private Predicate<? super Throwable> couldNotLockPredicate = e -> {
        int i = ExceptionUtils.indexOfType(e, PgException.class);
        if (i >= 0) {
            PgException e2 = (PgException) ExceptionUtils.getThrowables(e)[i];
            if ("55P03".equals(e2.getCode())) {
                return true;
            }
        }
        return false;
    };
}
