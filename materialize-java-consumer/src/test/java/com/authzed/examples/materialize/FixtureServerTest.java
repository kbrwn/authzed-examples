package com.authzed.examples.materialize;

import com.authzed.api.materialize.v0.WatchPermissionSetsRequest;
import com.authzed.api.materialize.v0.WatchPermissionSetsResponse;
import com.authzed.api.materialize.v0.WatchPermissionSetsServiceGrpc;
import com.authzed.api.v1.ZedToken;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(15)
class FixtureServerTest {
    @Test
    void emitsActualProtobufChangesAndCompletionBoundariesOverNetty() throws Exception {
        try (FixtureServer fixture = new FixtureServer(config(4, 0, 6))) {
            ManagedChannel channel = channel(fixture);
            try {
                Iterator<WatchPermissionSetsResponse> stream = stub(channel).watchPermissionSets(request(""));
                List<WatchPermissionSetsResponse> responses = new ArrayList<>();
                stream.forEachRemaining(responses::add);
                assertEquals(7, responses.size(), "six changes plus one completed revision");
                for (int i = 0; i < 4; i++) {
                    assertTrue(responses.get(i).hasChange());
                    assertEquals(FixtureServer.revisionToken(1), responses.get(i).getChange().getAtRevision().getToken());
                    assertTrue(Math.abs(responses.get(i).getChange().getSerializedSize() - 256) <= 1);
                }
                assertNotEquals(responses.get(0).getChange().getParentSet(), responses.get(1).getChange().getParentSet());
                assertTrue(responses.get(0).getChange().hasChildSet());
                assertTrue(responses.get(1).getChange().hasChildMember());
                assertEquals(FixtureServer.revisionToken(1), responses.get(4).getCompletedRevision().getToken());
                assertEquals(FixtureServer.revisionToken(2), responses.get(5).getChange().getAtRevision().getToken());
            } finally { close(channel); }
        }
    }

    @Test
    void rejectsConcurrentWatchersAndAcceptsReplacementAfterCancellation() throws Exception {
        try (FixtureServer fixture = new FixtureServer(config(4, 0, 0))) {
            ManagedChannel channel = channel(fixture);
            try (Context.CancellableContext context = Context.current().withCancellation()) {
                Iterator<WatchPermissionSetsResponse> first = context.call(() -> stub(channel).watchPermissionSets(request("")));
                assertTrue(first.next().hasChange());
                Iterator<WatchPermissionSetsResponse> second = stub(channel).watchPermissionSets(request(""));
                StatusRuntimeException rejected = assertThrows(StatusRuntimeException.class, second::hasNext);
                assertEquals(Status.Code.RESOURCE_EXHAUSTED, rejected.getStatus().getCode());
                context.cancel(null);
                awaitInactive(fixture);
                try (Context.CancellableContext replacement = Context.current().withCancellation()) {
                    Iterator<WatchPermissionSetsResponse> resumed = replacement.call(() -> stub(channel)
                            .watchPermissionSets(request(FixtureServer.revisionToken(3))));
                    assertEquals(FixtureServer.revisionToken(4), resumed.next().getChange().getAtRevision().getToken());
                }
            } finally { close(channel); }
        }
    }

    @Test
    void disconnectMidRevisionReplaysFromLastCompletedRevision() throws Exception {
        try (FixtureServer fixture = new FixtureServer(config(3, 4, 0))) {
            ManagedChannel channel = channel(fixture);
            try {
                Iterator<WatchPermissionSetsResponse> stream = stub(channel).watchPermissionSets(request(""));
                for (int i = 0; i < 3; i++) assertTrue(stream.next().hasChange());
                String durableToken = stream.next().getCompletedRevision().getToken();
                WatchPermissionSetsResponse unfinished = stream.next();
                StatusRuntimeException disconnect = assertThrows(StatusRuntimeException.class, stream::hasNext);
                assertEquals(Status.Code.UNAVAILABLE, disconnect.getStatus().getCode());
                awaitInactive(fixture);
                try (Context.CancellableContext context = Context.current().withCancellation()) {
                    Iterator<WatchPermissionSetsResponse> resumed = context.call(() -> stub(channel)
                            .watchPermissionSets(request(durableToken)));
                    assertEquals(unfinished, resumed.next(), "unfinished revision must replay from its first event");
                }
            } finally { close(channel); }
        }
    }

    @Test
    void serverSupportsClientManualDemandWithoutSendingCallbacksEarly() throws Exception {
        try (FixtureServer fixture = new FixtureServer(config(100, 0, 0))) {
            ManagedChannel channel = channel(fixture);
            AtomicReference<ClientCallStreamObserver<WatchPermissionSetsRequest>> call = new AtomicReference<>();
            CountDownLatch started = new CountDownLatch(1), first = new CountDownLatch(1);
            AtomicInteger callbacks = new AtomicInteger();
            try {
                WatchPermissionSetsServiceGrpc.newStub(channel).watchPermissionSets(request(""),
                        new ClientResponseObserver<WatchPermissionSetsRequest, WatchPermissionSetsResponse>() {
                            @Override public void beforeStart(ClientCallStreamObserver<WatchPermissionSetsRequest> stream) {
                                stream.disableAutoRequestWithInitial(0);
                                call.set(stream);
                                started.countDown();
                            }
                            @Override public void onNext(WatchPermissionSetsResponse value) {
                                callbacks.incrementAndGet(); first.countDown();
                            }
                            @Override public void onError(Throwable error) { }
                            @Override public void onCompleted() { }
                        });
                assertTrue(started.await(3, TimeUnit.SECONDS));
                assertFalse(first.await(150, TimeUnit.MILLISECONDS), "no application messages without demand");
                call.get().request(1);
                assertTrue(first.await(3, TimeUnit.SECONDS));
                assertEquals(1, callbacks.get());
                call.get().cancel("test finished", null);
                awaitInactive(fixture);
            } finally { close(channel); }
        }
    }

    @Test
    void rejectsNonFixtureResumeTokens() throws Exception {
        try (FixtureServer fixture = new FixtureServer(config(4, 0, 6))) {
            ManagedChannel channel = channel(fixture);
            try {
                Iterator<WatchPermissionSetsResponse> stream = stub(channel).watchPermissionSets(request("production-token"));
                StatusRuntimeException error = assertThrows(StatusRuntimeException.class, stream::hasNext);
                assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
                assertFalse(fixture.hasActiveWatcher());
            } finally { close(channel); }
        }
    }

    private static FixtureServer.Config config(int revisionSize, long disconnect, long maxEvents) {
        return new FixtureServer.Config(0, revisionSize, 256, 8192, 0, 0, 0, disconnect, maxEvents);
    }

    private static ManagedChannel channel(FixtureServer fixture) {
        return NettyChannelBuilder.forAddress("127.0.0.1", fixture.port()).usePlaintext().build();
    }

    private static WatchPermissionSetsServiceGrpc.WatchPermissionSetsServiceBlockingStub stub(ManagedChannel channel) {
        return WatchPermissionSetsServiceGrpc.newBlockingStub(channel).withDeadlineAfter(8, TimeUnit.SECONDS);
    }

    private static WatchPermissionSetsRequest request(String revision) {
        WatchPermissionSetsRequest.Builder request = WatchPermissionSetsRequest.newBuilder();
        if (!revision.isEmpty()) request.setOptionalStartingAfter(ZedToken.newBuilder().setToken(revision));
        return request.build();
    }

    private static void awaitInactive(FixtureServer fixture) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (fixture.hasActiveWatcher() && System.nanoTime() < deadline) Thread.sleep(5);
        assertFalse(fixture.hasActiveWatcher(), "cancelled watch remained active");
    }

    private static void close(ManagedChannel channel) throws InterruptedException {
        channel.shutdownNow(); channel.awaitTermination(3, TimeUnit.SECONDS);
    }
}
