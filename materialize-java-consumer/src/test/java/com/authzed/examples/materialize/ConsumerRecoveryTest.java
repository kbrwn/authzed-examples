package com.authzed.examples.materialize;

import com.authzed.api.materialize.v0.MemberReference;
import com.authzed.api.materialize.v0.PermissionSetChange;
import com.authzed.api.materialize.v0.SetReference;
import com.authzed.api.materialize.v0.WatchPermissionSetsRequest;
import com.authzed.api.materialize.v0.WatchPermissionSetsServiceGrpc;
import com.authzed.api.v1.ZedToken;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** End-to-end restart coverage: generated API, actual HTTP/2, ACK fences and durable file state. */
@Timeout(30)
class ConsumerRecoveryTest {
    @TempDir Path temporary;

    @Test
    void resumesOneWatcherFromDurableCheckpointAndReplaysUnfinishedRevision() throws Exception {
        int revisionSize = 100;
        var fixtureConfig = new FixtureServer.Config(0, revisionSize, 256, revisionSize,
                0, 0, 0, 150, 0);
        var consumerConfig = new AsyncMaterializeConsumer.Config(64, 64 * 1024,
                1024, 16, 17, 3000);
        Map<Long, Set<EventKey>> acknowledged = new HashMap<>();
        List<Long> savedRevisions = new ArrayList<>();
        AtomicInteger duplicateAcknowledgments = new AtomicInteger();
        Path checkpointFile = temporary.resolve("watch.checkpoint");

        try (FixtureServer fixture = new FixtureServer(fixtureConfig)) {
            String identity = "local-fixture:" + fixture.port() + "/immediate-ack-test";
            for (int attempt = 1; attempt <= 3; attempt++) {
                // A new channel and store deliberately model a replacement process/pod. The previous
                // RPC and channel must terminate before this loop opens the next connection.
                assertFalse(fixture.hasActiveWatcher(), "previous watch must finish before reconnecting");
                ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", fixture.port())
                        .usePlaintext().build();
                AsyncMaterializeConsumer.Metrics metrics = new AsyncMaterializeConsumer.Metrics();
                try (FileCheckpointStore store = new FileCheckpointStore(checkpointFile, identity)) {
                    ZedToken previous = store.load();
                    assertEquals(attempt - 1, previous == null ? 0 : revision(previous));
                    WatchPermissionSetsRequest.Builder request = WatchPermissionSetsRequest.newBuilder();
                    if (previous != null) request.setOptionalStartingAfter(previous);

                    AsyncMaterializeConsumer.Publisher sink = (changes, completion) -> {
                        for (PermissionSetChange change : changes) {
                            long atRevision = revision(change.getAtRevision());
                            Set<EventKey> events = acknowledged.computeIfAbsent(atRevision,
                                    ignored -> new HashSet<>());
                            if (!events.add(EventKey.of(change))) duplicateAcknowledgments.incrementAndGet();
                        }
                        // Synthetic downstream ACK: this validates recovery bookkeeping, not a real
                        // destination's durability or speed. All covered events precede completion.
                        completion.complete(null);
                    };
                    AsyncMaterializeConsumer.CheckpointStore verifyingStore = completed -> {
                        long next = revision(completed);
                        long previousSaved = savedRevisions.isEmpty() ? 0
                                : savedRevisions.get(savedRevisions.size() - 1);
                        assertTrue(next > previousSaved, "durable checkpoints must advance monotonically");
                        assertCompleteCoverage(acknowledged, next, revisionSize);
                        store.save(completed);
                        assertEquals(completed, store.load(), "checkpoint must survive a new file read");
                        savedRevisions.add(next);
                    };

                    StatusRuntimeException disconnected = assertThrows(StatusRuntimeException.class,
                            () -> AsyncMaterializeConsumer.run(WatchPermissionSetsServiceGrpc.newStub(channel)
                                            .withDeadlineAfter(5, TimeUnit.SECONDS),
                                    request.build(), sink, verifyingStore, consumerConfig, metrics));
                    assertEquals(Status.Code.UNAVAILABLE, disconnected.getStatus().getCode());
                    assertEquals(1, metrics.completedRevisions.get());
                    assertEquals(revisionSize, metrics.changesCheckpointed.get());
                    assertEquals(metrics.changesReceived.get(), metrics.changesAcknowledged.get(),
                            "source failure must drain received work before returning in this ACK-success case");
                    assertTrue(metrics.changesAcknowledged.get() > revisionSize,
                            "test must acknowledge part of the next revision before the disconnect");
                    assertEquals(0, metrics.pendingBatches.get());
                    assertEquals(0, metrics.inFlightResponses.get());
                    assertEquals(0, metrics.inFlightBytes.get());
                    assertEquals(attempt, revision(store.load()));
                } finally {
                    channel.shutdownNow();
                    assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS), "old channel did not terminate");
                }
                awaitInactive(fixture);
            }

            try (FileCheckpointStore restarted = new FileCheckpointStore(checkpointFile, identity)) {
                assertEquals(3, revision(restarted.load()));
                assertCompleteCoverage(acknowledged, revision(restarted.load()), revisionSize);
            }
            assertEquals(List.of(1L, 2L, 3L), savedRevisions);
            assertTrue(duplicateAcknowledgments.get() > 0,
                    "unfinished revision must replay even though some of its events were acknowledged");
            assertEquals(50, acknowledged.get(4L).size(), "final partial revision is not checkpointed");
            assertFalse(fixture.hasActiveWatcher());
        }
    }

    private static void assertCompleteCoverage(Map<Long, Set<EventKey>> acknowledged,
                                              long completedRevision, int expectedPerRevision) {
        for (long revision = 1; revision <= completedRevision; revision++) {
            Set<EventKey> events = acknowledged.get(revision);
            assertNotNull(events, "checkpoint covers a revision with no acknowledged events: " + revision);
            assertEquals(expectedPerRevision, events.size(),
                    "checkpoint covers missing/duplicate-replaced events in revision " + revision);
        }
    }

    private record EventKey(String revision, SetReference parent, SetReference childSet,
                            MemberReference childMember, PermissionSetChange.SetOperation operation) {
        static EventKey of(PermissionSetChange change) {
            return new EventKey(change.getAtRevision().getToken(), change.getParentSet(), change.getChildSet(),
                    change.getChildMember(), change.getOperation());
        }
    }

    // Parsing is confined to our deliberately synthetic fixture token format. Production ZedTokens
    // remain opaque everywhere in the implementation and checkpoint store.
    private static long revision(ZedToken token) {
        assertNotNull(token);
        String prefix = "fixture-revision-";
        assertTrue(token.getToken().startsWith(prefix));
        return Long.parseLong(token.getToken().substring(prefix.length()));
    }

    private static void awaitInactive(FixtureServer fixture) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (fixture.hasActiveWatcher() && System.nanoTime() < deadline) Thread.sleep(5);
        assertFalse(fixture.hasActiveWatcher(), "old RPC remains active after consumer and channel termination");
    }
}
