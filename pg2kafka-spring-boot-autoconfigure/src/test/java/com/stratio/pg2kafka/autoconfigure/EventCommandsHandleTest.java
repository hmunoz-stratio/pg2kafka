package com.stratio.pg2kafka.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import io.reactiverse.pgclient.PgException;
import io.reactiverse.pgclient.impl.codec.decoder.ErrorResponse;
import io.reactiverse.reactivex.pgclient.PgRowSet;
import io.reactiverse.reactivex.pgclient.PgTransaction;
import io.reactiverse.reactivex.pgclient.Tuple;
import io.reactivex.Single;
import reactor.test.StepVerifier;

public class EventCommandsHandleTest {

    static final String TEST_TABLE = "test_table";

    EventCommandsHandler classUnderTest = new EventCommandsHandler(TEST_TABLE);

    /***** tryAcquire tests *****/

    @Test
    public void givenNullTx_whenTryAcquire_thenIllegalArgumentExceptionIsThrown() {
        assertThatThrownBy(() -> classUnderTest.tryAcquire(null, 1l)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void givenAnEventIdThatCanBeAcquired_whenTryAcquire_thenReturnsAMonoThatEmitsTrue() {
        PgTransaction txMock = mock(PgTransaction.class);
        PgRowSet pgRowSetMock = mock(PgRowSet.class);
        when(txMock.rxPreparedQuery(anyString(), eq(Tuple.of(1l)))).thenReturn(Single.just(pgRowSetMock));
        when(pgRowSetMock.rowCount()).thenReturn(1);

        StepVerifier.create(classUnderTest.tryAcquire(txMock, 1l))
                .expectNext(true)
                .verifyComplete();

        verify(txMock).rxPreparedQuery(anyString(), eq(Tuple.of(1l)));
        verify(pgRowSetMock).rowCount();
    }

    @Test
    public void givenAnEventIdThatCannotBeAcquired_whenTryAcquire_thenReturnsAMonoThatEmitsFalse() {
        PgTransaction txMock = mock(PgTransaction.class);
        PgRowSet pgRowSetMock = mock(PgRowSet.class);
        when(txMock.rxPreparedQuery(anyString(), eq(Tuple.of(1l)))).thenReturn(Single.just(pgRowSetMock));
        when(pgRowSetMock.rowCount()).thenReturn(0);

        StepVerifier.create(classUnderTest.tryAcquire(txMock, 1l))
                .expectNext(false)
                .verifyComplete();

        verify(txMock).rxPreparedQuery(anyString(), eq(Tuple.of(1l)));
        verify(pgRowSetMock).rowCount();
    }

    @Test
    public void givenAnEventIdThatLockNotAvailable_whenTryAcquire_thenReturnsAMonoThatEmitsFalse() {
        PgTransaction txMock = mock(PgTransaction.class);
        ErrorResponse errResponse = new ErrorResponse();
        errResponse.setCode("55P03");
        when(txMock.rxPreparedQuery(anyString(), eq(Tuple.of(1l))))
                .thenReturn(Single.error(new PgException(errResponse)));

        StepVerifier.create(classUnderTest.tryAcquire(txMock, 1l))
                .expectNext(false)
                .verifyComplete();

        verify(txMock).rxPreparedQuery(anyString(), eq(Tuple.of(1l)));
    }

    @Test
    public void whenTryAcquire_thenSqlContainsTableName() {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        PgTransaction txMock = mock(PgTransaction.class);
        PgRowSet pgRowSetMock = mock(PgRowSet.class);
        when(txMock.rxPreparedQuery(captor.capture(), eq(Tuple.of(1l)))).thenReturn(Single.just(pgRowSetMock));
        when(pgRowSetMock.rowCount()).thenReturn(1);

        StepVerifier.create(classUnderTest.tryAcquire(txMock, 1l))
                .expectNextCount(1)
                .verifyComplete();
        
        verify(txMock).rxPreparedQuery(anyString(), eq(Tuple.of(1l)));

        assertThat(captor.getValue()).contains(TEST_TABLE);

    }

    /***** markProcessed tests *****/

    @Test
    public void givenNullTx_whenMarkProcessed_thenIllegalArgumentExceptionIsThrown() {
        assertThatThrownBy(() -> classUnderTest.markProcessed(null, 1l)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void givenAnEventId_whenMarkProcessed_thenSqlUpdateIsPrepared() {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        PgTransaction txMock = mock(PgTransaction.class);
        PgRowSet pgRowSetMock = mock(PgRowSet.class);
        when(txMock.rxPreparedQuery(captor.capture(), eq(Tuple.of(1l)))).thenReturn(Single.just(pgRowSetMock));

        StepVerifier.create(classUnderTest.markProcessed(txMock, 1l))
                .verifyComplete();

        verify(txMock).rxPreparedQuery(anyString(), eq(Tuple.of(1l)));

        assertThat(captor.getValue().toLowerCase()).startsWith(("UPDATE " + TEST_TABLE).toLowerCase());
    }

}
