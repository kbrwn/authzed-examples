package com.authzed.examples.materialize;

import static org.junit.jupiter.api.Assertions.*;

import io.grpc.Status;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class ConsumerMainTest {
    @Test void transientSourceFailuresCanRetryWhenCleanupSucceeded() {
        assertTrue(ConsumerMain.transientFailure(new IOException("Watch ended")));
        assertTrue(ConsumerMain.transientFailure(Status.UNAVAILABLE.asRuntimeException()));
        assertTrue(ConsumerMain.transientFailure(Status.DEADLINE_EXCEEDED.asRuntimeException()));
    }

    @Test void anRpcTerminalCallbackTimeoutPreventsRetryEvenWithARetryablePrimaryFailure() {
        for (var sourceFailure : new Throwable[] {
                new IOException("Watch disconnected"), Status.UNAVAILABLE.asRuntimeException()}) {
            sourceFailure.addSuppressed(new IllegalStateException(
                    "RPC cancellation did not terminate callbacks; do not restart concurrently"));
            assertFalse(ConsumerMain.transientFailure(sourceFailure),
                    "A replacement Watch must not hide failure to terminate the previous attempt");
        }
    }

    @Test void destinationCleanupFailuresAndPermanentFailuresStopInsteadOfRetrying() {
        var sourceFailure = new IOException("Watch disconnected");
        sourceFailure.addSuppressed(new IOException("Destination close did not finish its pending writes"));
        assertFalse(ConsumerMain.transientFailure(sourceFailure));
        assertFalse(ConsumerMain.transientFailure(Status.PERMISSION_DENIED.asRuntimeException()));
        assertFalse(ConsumerMain.transientFailure(Status.INVALID_ARGUMENT.asRuntimeException()));
        assertFalse(ConsumerMain.transientFailure(new InterruptedException("Shutdown requested")));
    }
}
