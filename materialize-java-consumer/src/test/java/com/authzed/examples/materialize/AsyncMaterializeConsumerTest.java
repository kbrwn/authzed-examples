package com.authzed.examples.materialize;

import static org.junit.jupiter.api.Assertions.*;

import com.authzed.api.materialize.v0.BreakingSchemaChange;
import com.authzed.api.materialize.v0.LookupPermissionSetsRequired;
import com.authzed.api.materialize.v0.MemberReference;
import com.authzed.api.materialize.v0.PermissionSetChange;
import com.authzed.api.materialize.v0.SetReference;
import com.authzed.api.materialize.v0.WatchPermissionSetsRequest;
import com.authzed.api.materialize.v0.WatchPermissionSetsResponse;
import com.authzed.api.materialize.v0.WatchPermissionSetsServiceGrpc;
import com.authzed.api.v1.ZedToken;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class AsyncMaterializeConsumerTest {
    @Test void aBatchLargerThanTheEntireDemandWindowCannotDeadlock() throws Exception {
        var events = List.of(change("one"), change("two"), completed("r1"));
        var order = new ArrayList<String>();
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(events, null, false)) {
            assertThrows(IOException.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(),
                    (batch, done) -> {
                        for (var change : batch) order.add(change.getParentSet().getObjectId());
                        done.complete(null);
                    }, token -> order.add(token.getToken()), config(1, 1024, 1024, 1, 100), metrics));
            assertEquals(List.of("one", "two", "r1"), order);
            assertEquals(3, metrics.responsesReceived.get());
            assertEquals(2, metrics.changesReceived.get());
            assertEquals(2, metrics.changesAcknowledged.get());
            assertEquals(0, metrics.inFlightResponses.get());
            assertEquals(0, metrics.inFlightBytes.get());
            assertEquals(1, rpc.calls.get());
        }
    }

    @Test void checkpointWaitsForEveryPriorBatchEvenWhenAcknowledgmentsAreOutOfOrder() throws Exception {
        var callbacks = new LinkedBlockingQueue<AsyncMaterializeConsumer.Completion>();
        var checkpoint = new CountDownLatch(1);
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(List.of(change("one"), change("two"), completed("r1")), null, false);
             var attempt = new Attempt(rpc, (batch, done) -> callbacks.add(done),
                     token -> checkpoint.countDown(), config(4, 4096, 1024, 4, 1), metrics)) {
            var first = take(callbacks);
            var second = take(callbacks);
            second.complete(null);
            assertFalse(checkpoint.await(100, TimeUnit.MILLISECONDS));
            assertEquals(1, metrics.changesAcknowledged.get());
            first.complete(null);
            assertTrue(checkpoint.await(2, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, attempt.failure());
            assertEquals(1, metrics.completedRevisions.get());
        }
    }

    @Test void laterRevisionsPublishWhileEarlierAcksWaitButCheckpointOnlyTheAcknowledgedPrefix() throws Exception {
        var callbacks = new LinkedBlockingQueue<AsyncMaterializeConsumer.Completion>();
        var saved = new ArrayList<String>();
        var priorFrontiers = new ArrayList<Long>();
        var metrics = new AsyncMaterializeConsumer.Metrics();
        var events = List.of(change("one"), completed("r1"), change("two"), completed("r2"),
                change("three"), completed("r3"));
        try (var rpc = new Rpc(events, null, false);
             var attempt = new Attempt(rpc, (batch, done) -> callbacks.add(done), token -> {
                 saved.add(token.getToken());
                 priorFrontiers.add(metrics.changesCheckpointed.get());
             }, config(8, 8192, 1024, 8, 1), metrics)) {
            // All three revisions must reach the downstream before the first batch is acknowledged.
            var first = take(callbacks);
            var second = take(callbacks);
            var third = take(callbacks);
            await(() -> metrics.pendingCheckpoints.get() == 3);
            third.complete(null);
            second.complete(null);
            assertEquals(0, metrics.completedRevisions.get());
            assertEquals(0, metrics.changesCheckpointed.get());
            first.complete(null);
            assertInstanceOf(IOException.class, attempt.failure());
            assertEquals(List.of("r1", "r2", "r3"), saved);
            // Even though all three records ACKed, saving r1 covers one event, not three.
            assertEquals(List.of(0L, 1L, 2L), priorFrontiers);
            assertEquals(3, metrics.changesCheckpointed.get());
            assertEquals(0, metrics.pendingCheckpoints.get());
            assertEquals(0, metrics.inFlightResponses.get());
        }
    }

    @Test void aPartialBatchFlushesOnItsLatencyDeadlineWithoutEofOrCreditExhaustion() throws Exception {
        var published = new CountDownLatch(1);
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(List.of(change("one")), null, true);
             var attempt = new Attempt(rpc, (batch, done) -> {
                 assertEquals(1, batch.size());
                 done.complete(null);
                 published.countDown();
             }, token -> fail("No revision marker was received"), config(8, 8192, 1024, 8, 100), metrics)) {
            assertTrue(published.await(2, TimeUnit.SECONDS), "Low-rate changes must not wait for a full batch");
            assertEquals(1, metrics.changesAcknowledged.get());
            assertEquals(0, metrics.changesCheckpointed.get());
        }
    }

    @Test void aSynchronousPublishFailureAtAMarkerReleasesItsControlCredit() throws Exception {
        var expected = new IOException("Submission failed");
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(List.of(change("one"), completed("unsafe")), null, false)) {
            var actual = assertThrows(IOException.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(),
                    (batch, done) -> { throw expected; }, token -> fail("Failed batch cannot checkpoint"),
                    config(8, 8192, 1024, 8, 100), metrics));
            assertSame(expected, actual);
            assertEquals(0, metrics.inFlightResponses.get());
            assertEquals(0, metrics.inFlightBytes.get());
            assertEquals(0, metrics.pendingCheckpoints.get());
        }
    }

    @Test void aStalledOldestAckCannotGrowCompletedBatchMetadataWithoutBound() throws Exception {
        var first = new AtomicReference<AsyncMaterializeConsumer.Completion>();
        var fourthSubmitted = new CountDownLatch(1);
        var fifthSubmitted = new CountDownLatch(1);
        var submissions = new AtomicInteger();
        var events = new ArrayList<WatchPermissionSetsResponse>();
        for (int i = 0; i < 20; i++) events.add(change(Integer.toString(i)));
        events.add(completed("r1"));
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(events, null, false);
             var attempt = new Attempt(rpc, (batch, done) -> {
                 int number = submissions.incrementAndGet();
                 if (number == 1) first.set(done);
                 else done.complete(null);
                 if (number == 4) fourthSubmitted.countDown();
                 if (number == 5) fifthSubmitted.countDown();
             }, token -> {}, config(4, 4096, 1024, 4, 1), metrics)) {
            assertTrue(fourthSubmitted.await(2, TimeUnit.SECONDS));
            // Three later ACKs free payload credits, but their metadata cannot retire past batch 1.
            assertFalse(fifthSubmitted.await(100, TimeUnit.MILLISECONDS));
            assertEquals(4, metrics.retainedBatchEntries.get());
            assertEquals(1, metrics.pendingBatches.get());
            assertTrue(metrics.maxObservedRetainedBatchEntries.get() <= 4);
            first.get().complete(null);
            assertTrue(fifthSubmitted.await(2, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, attempt.failure());
            assertEquals(20, metrics.changesCheckpointed.get());
            assertEquals(0, metrics.retainedBatchEntries.get());
            assertTrue(metrics.maxObservedRetainedBatchEntries.get() <= 4);
        }
    }

    @Test void byteReservationsAndUnacknowledgedRecordsStayWithinBothLimits() throws Exception {
        var events = new ArrayList<WatchPermissionSetsResponse>();
        for (int i = 0; i < 100; i++) events.add(change("x".repeat(80) + i));
        events.add(completed("last"));
        var held = new LinkedBlockingQueue<AsyncMaterializeConsumer.Completion>();
        var release = new AtomicBoolean();
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(events, null, false);
             var attempt = new Attempt(rpc, (batch, done) -> {
                 if (release.get()) done.complete(null);
                 else {
                     held.add(done);
                     if (release.get() && held.remove(done)) done.complete(null);
                 }
             }, token -> {}, config(8, 1024, 256, 8, 2), metrics)) {
            await(() -> metrics.pendingBatches.get() >= 2);
            await(() -> metrics.inFlightBytes.get() > 768);
            long count = metrics.responsesReceived.get();
            // Budget exhaustion, rather than an unbounded Java/downstream queue, stalls receiving.
            assertTrue(count <= 8);
            assertTrue(metrics.maxObservedInFlightResponses.get() <= 8);
            assertTrue(metrics.maxObservedInFlightBytes.get() <= 1024);
            assertTrue(metrics.reservedBytes.get() >= 0);
            release.set(true);
            AsyncMaterializeConsumer.Completion done;
            while ((done = held.poll()) != null) done.complete(null);
            assertInstanceOf(IOException.class, attempt.failure());
            assertEquals(100, metrics.changesAcknowledged.get());
            assertEquals(101, metrics.responsesReceived.get());
            assertEquals(0, metrics.inFlightResponses.get());
            assertEquals(0, metrics.inFlightBytes.get());
            assertEquals(0, metrics.reservedBytes.get());
            assertTrue(metrics.maxObservedInFlightResponses.get() <= 8);
            assertTrue(metrics.maxObservedInFlightBytes.get() <= 1024);
        }
    }

    @Test void controlMessagesConsumeCreditsAndReleaseThemAfterCheckpointing() throws Exception {
        var metrics = new AsyncMaterializeConsumer.Metrics();
        var saves = new ArrayList<String>();
        var events = List.of(completed("r1"), completed("r2"), change("one"), completed("r3"));
        try (var rpc = new Rpc(events, null, false)) {
            assertThrows(IOException.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(),
                    (batch, done) -> done.complete(null), token -> {
                        assertEquals(1, metrics.inFlightResponses.get());
                        assertEquals(0, metrics.reservedResponses.get());
                        saves.add(token.getToken());
                    }, config(1, 1024, 1024, 10, 10), metrics));
            assertEquals(List.of("r1", "r2", "r3"), saves);
            assertEquals(4, metrics.responsesReceived.get());
            assertEquals(1, metrics.changesReceived.get());
        }
    }

    @Test void downstreamFailureCancelsTheActualRpcAndDoesNotCheckpoint() throws Exception {
        var expected = new IOException("Downstream unavailable");
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(List.of(change("one"), completed("unsafe")), null, true)) {
            var actual = assertThrows(IOException.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(),
                    (batch, done) -> done.complete(expected), token -> fail("Must not save"),
                    config(2, 2048, 1024, 2, 1), metrics));
            assertSame(expected, actual);
            assertTrue(rpc.cancelled.await(2, TimeUnit.SECONDS));
            assertEquals(0, metrics.completedRevisions.get());
            assertEquals(0, metrics.inFlightResponses.get());
            assertEquals(0, metrics.inFlightBytes.get());
        }
    }

    @Test void missingAcknowledgmentTimesOutAndCancelsInsteadOfHangingForever() throws Exception {
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(List.of(change("one")), null, true)) {
            assertThrows(TimeoutException.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(),
                    (batch, done) -> {}, token -> fail("No completion received"),
                    new AsyncMaterializeConsumer.Config(1, 1024, 1024, 1, 10, 100), metrics));
            assertTrue(rpc.cancelled.await(2, TimeUnit.SECONDS));
            assertEquals(0, metrics.completedRevisions.get());
        }
    }

    @Test void checkpointFailureStopsBeforePublishingLaterRevisions() throws Exception {
        var expected = new IOException("checkpoint disk unavailable");
        var published = new ArrayList<String>();
        try (var rpc = new Rpc(List.of(change("before"), completed("r1"), change("after")), null, false)) {
            var actual = assertThrows(IOException.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(),
                    (batch, done) -> {
                        batch.forEach(change -> published.add(change.getParentSet().getObjectId()));
                        done.complete(null);
                    }, token -> { throw expected; }, config(8, 8192, 1024, 8, 8),
                    new AsyncMaterializeConsumer.Metrics()));
            assertSame(expected, actual);
            assertEquals(List.of("before"), published);
        }
    }

    @Test void transportFailureDrainsEarlierCompletedRevisionButDoesNotInventANewOne() throws Exception {
        var published = new ArrayList<String>();
        var saved = new ArrayList<String>();
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(List.of(change("before"), completed("r1"), change("unfinished")), null, true)) {
            assertThrows(io.grpc.StatusRuntimeException.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(),
                    (batch, done) -> {
                        batch.forEach(change -> published.add(change.getParentSet().getObjectId()));
                        done.complete(null);
                        if (published.contains("unfinished")) rpc.observer.get().onError(Status.UNAVAILABLE.asRuntimeException());
                    }, token -> saved.add(token.getToken()), config(8, 8192, 1024, 8, 8), metrics));
            assertEquals(List.of("r1"), saved);
            assertEquals(List.of("before", "unfinished"), published);
            assertEquals(metrics.changesPublished.get(), metrics.changesAcknowledged.get());
        }
    }

    @Test void anOversizedResponseCannotEnterTheApplicationBudget() throws Exception {
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(List.of(change("x".repeat(1024))), null, true)) {
            assertThrows(Exception.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(),
                    (batch, done) -> fail("Oversized response must not reach publisher"),
                    token -> fail("No checkpoint expected"), config(4, 512, 128, 4, 4), metrics));
            assertEquals(0, metrics.changesPublished.get());
            assertEquals(0, metrics.inFlightBytes.get());
            assertTrue(metrics.maxObservedInFlightBytes.get() <= 512);
        }
    }

    @Test void schemaControlStopsWithTheOriginalControlAndNoInventedCheckpoint() throws Exception {
        var control = WatchPermissionSetsResponse.newBuilder().setLookupPermissionSetsRequired(
                LookupPermissionSetsRequired.newBuilder().setRequiredLookupAt(token("snapshot"))).build();
        var published = new ArrayList<String>();
        try (var rpc = new Rpc(List.of(change("before"), control, change("after")), null, false)) {
            var actual = assertThrows(RebuildRequiredException.class,
                    () -> AsyncMaterializeConsumer.run(rpc.stub(), request(), (batch, done) -> {
                        batch.forEach(change -> published.add(change.getParentSet().getObjectId()));
                        done.complete(null);
                    }, token -> fail("A schema revision is not a completed checkpoint"),
                            config(8, 8192, 1024, 8, 8), new AsyncMaterializeConsumer.Metrics()));
            assertEquals(control, actual.control());
            assertEquals(List.of("before"), published);
        }
    }

    @Test void breakingSchemaWarningContinuesUntilTheLookupRequiredBoundary() throws Exception {
        var warning = WatchPermissionSetsResponse.newBuilder().setBreakingSchemaChange(
                BreakingSchemaChange.newBuilder().setChangeAt(token("schema"))).build();
        var rebuild = WatchPermissionSetsResponse.newBuilder().setLookupPermissionSetsRequired(
                LookupPermissionSetsRequired.newBuilder().setRequiredLookupAt(token("snapshot"))).build();
        var metrics = new AsyncMaterializeConsumer.Metrics();
        var published = new ArrayList<String>();
        try (var rpc = new Rpc(List.of(change("before"), warning, change("after-warning"), rebuild), null, false)) {
            var actual = assertThrows(RebuildRequiredException.class,
                    () -> AsyncMaterializeConsumer.run(rpc.stub(), request(), (batch, done) -> {
                        batch.forEach(change -> published.add(change.getParentSet().getObjectId()));
                        done.complete(null);
                    }, token -> fail("Warnings and lookup tokens are not completed revisions"),
                            config(1, 1024, 1024, 1, 100), metrics));
            assertEquals(rebuild, actual.control());
            assertEquals(List.of("before", "after-warning"), published);
            assertEquals(1, metrics.breakingSchemaChanges.get());
            assertEquals(0, metrics.completedRevisions.get());
            assertEquals(0, metrics.inFlightResponses.get());
        }
    }

    @Test void duplicateCompletionCannotFreeCreditsTwice() throws Exception {
        var metrics = new AsyncMaterializeConsumer.Metrics();
        try (var rpc = new Rpc(List.of(change("one"), completed("r1")), null, false)) {
            assertThrows(IOException.class, () -> AsyncMaterializeConsumer.run(rpc.stub(), request(), (batch, done) -> {
                done.complete(null);
                done.complete(null);
            }, token -> {}, config(1, 1024, 1024, 1, 1), metrics));
            assertEquals(1, metrics.changesAcknowledged.get());
            assertEquals(0, metrics.inFlightBytes.get());
            assertEquals(0, metrics.inFlightResponses.get());
        }
    }

    private static AsyncMaterializeConsumer.Config config(int count, long bytes, int maxResponse,
                                                          int demand, int batch) {
        return new AsyncMaterializeConsumer.Config(count, bytes, maxResponse, demand, batch, 3000);
    }
    private static WatchPermissionSetsRequest request() { return WatchPermissionSetsRequest.getDefaultInstance(); }
    private static ZedToken token(String value) { return ZedToken.newBuilder().setToken(value).build(); }
    private static WatchPermissionSetsResponse completed(String revision) {
        return WatchPermissionSetsResponse.newBuilder().setCompletedRevision(token(revision)).build();
    }
    private static WatchPermissionSetsResponse change(String id) {
        return WatchPermissionSetsResponse.newBuilder().setChange(PermissionSetChange.newBuilder()
                .setAtRevision(token("r"))
                .setOperation(PermissionSetChange.SetOperation.SET_OPERATION_ADDED)
                .setParentSet(SetReference.newBuilder().setObjectType("document")
                        .setObjectId(id).setPermissionOrRelation("view"))
                .setChildMember(MemberReference.newBuilder().setObjectType("user").setObjectId("alice")))
                .build();
    }
    private static AsyncMaterializeConsumer.Completion take(
            LinkedBlockingQueue<AsyncMaterializeConsumer.Completion> queue) throws InterruptedException {
        var item = queue.poll(3, TimeUnit.SECONDS);
        assertNotNull(item, "Publisher did not receive its batch");
        return item;
    }
    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(2);
        assertTrue(condition.getAsBoolean(), "Condition did not become true");
    }

    private static final class Attempt implements AutoCloseable {
        final CompletableFuture<Throwable> outcome = new CompletableFuture<>();
        final Thread thread;
        Attempt(Rpc rpc, AsyncMaterializeConsumer.Publisher publisher, AsyncMaterializeConsumer.CheckpointStore store,
                AsyncMaterializeConsumer.Config config, AsyncMaterializeConsumer.Metrics metrics) {
            thread = new Thread(() -> {
                try {
                    AsyncMaterializeConsumer.run(rpc.stub(), request(), publisher, store, config, metrics);
                    outcome.complete(new AssertionError("Watch unexpectedly returned normally"));
                } catch (Throwable failure) {
                    outcome.complete(failure);
                }
            }, "test-publisher");
            thread.start();
        }
        Throwable failure() throws Exception { return outcome.get(5, TimeUnit.SECONDS); }
        @Override public void close() throws Exception {
            thread.interrupt();
            thread.join(2000);
            assertFalse(thread.isAlive(), "Consumer leaked a thread");
        }
    }

    private static final class Rpc implements AutoCloseable {
        final ExecutorService callbacks = Executors.newSingleThreadExecutor();
        final CountDownLatch cancelled = new CountDownLatch(1);
        final AtomicInteger calls = new AtomicInteger();
        final AtomicReference<StreamObserver<WatchPermissionSetsResponse>> observer = new AtomicReference<>();
        final Server server;
        final ManagedChannel channel;
        Rpc(List<WatchPermissionSetsResponse> events, Throwable error, boolean remainOpen) throws Exception {
            String name = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(name).directExecutor().addService(
                    new WatchPermissionSetsServiceGrpc.WatchPermissionSetsServiceImplBase() {
                        @Override public void watchPermissionSets(WatchPermissionSetsRequest request,
                                StreamObserver<WatchPermissionSetsResponse> observer) {
                            calls.incrementAndGet();
                            Rpc.this.observer.set(observer);
                            Context.current().addListener(ignored -> cancelled.countDown(), Runnable::run);
                            events.forEach(observer::onNext);
                            if (error != null) observer.onError(error);
                            else if (!remainOpen) observer.onCompleted();
                        }
                    }).build().start();
            channel = InProcessChannelBuilder.forName(name).executor(callbacks).build();
        }
        WatchPermissionSetsServiceGrpc.WatchPermissionSetsServiceStub stub() {
            return WatchPermissionSetsServiceGrpc.newStub(channel);
        }
        @Override public void close() throws Exception {
            channel.shutdownNow();
            server.shutdownNow();
            assertTrue(channel.awaitTermination(2, TimeUnit.SECONDS));
            assertTrue(server.awaitTermination(2, TimeUnit.SECONDS));
            callbacks.shutdownNow();
            assertTrue(callbacks.awaitTermination(2, TimeUnit.SECONDS));
        }
    }
}
