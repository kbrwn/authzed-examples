package com.authzed.examples.materialize;

import com.authzed.api.materialize.v0.PermissionSetChange;
import com.authzed.api.materialize.v0.WatchPermissionSetsRequest;
import com.authzed.api.materialize.v0.WatchPermissionSetsResponse;
import com.authzed.api.materialize.v0.WatchPermissionSetsServiceGrpc;
import com.authzed.api.v1.ZedToken;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * One Materialize RPC, one serialized gRPC callback producer, and one publishing thread.
 * No per-event executor tasks or futures. The byte budget measures serialized protobuf bytes,
 * not Java heap: leave separate headroom for decoded objects, transport and downstream buffers.
 */
public final class AsyncMaterializeConsumer {
    private static final long MAX_BATCH_WAIT_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
    private AsyncMaterializeConsumer() {}

    public record Config(int maxInFlightResponses, long maxInFlightBytes, int maxResponseBytes,
                         int demandBatch, int publishBatch, long ackTimeoutMillis) {
        public Config {
            if (maxInFlightResponses < 1 || maxResponseBytes < 1
                    || maxInFlightBytes < maxResponseBytes || demandBatch < 1
                    || publishBatch < 1 || ackTimeoutMillis < 1) {
                throw new IllegalArgumentException("Positive limits required; byte budget must fit one maximum response");
            }
        }
    }

    @FunctionalInterface
    public interface Publisher {
        /**
         * Submit in list order and call completion exactly once, after ALL records have terminal
         * outcomes (acknowledged, failed, or cancelled and discarded). Report any failed record as
         * a failed batch. Do not release a batch on its first failure while retaining other records.
         * The immutable list may be retained until completion. Bound any blocking
         * inside this method using the downstream client's submission and delivery timeouts.
         * Preserve source order for conflicting changes ACROSS batches as well as within each
         * batch. Calls are submitted sequentially, but asynchronous completion and checkpoint
         * ordering cannot correct a destination that applies conflicting mutations out of order.
         */
        void publishBatch(List<PermissionSetChange> changes, Completion completion) throws Exception;
    }

    @FunctionalInterface
    public interface Completion { void complete(Throwable failure); }

    @FunctionalInterface
    public interface CheckpointStore {
        /** Durably save this opaque completed_revision; never a change's at_revision. */
        void save(ZedToken revision) throws Exception;
    }

    public static final class Metrics {
        public final AtomicLong responsesReceived = new AtomicLong();
        public final AtomicLong changesReceived = new AtomicLong();
        public final AtomicLong payloadBytesReceived = new AtomicLong();
        public final AtomicLong changesPublished = new AtomicLong();
        public final AtomicLong changesAcknowledged = new AtomicLong();
        public final AtomicLong completedRevisions = new AtomicLong();
        public final AtomicLong changesCheckpointed = new AtomicLong();
        public final AtomicLong pendingCheckpoints = new AtomicLong();
        public final AtomicLong breakingSchemaChanges = new AtomicLong();
        // These budgets include outstanding demand, queued responses and unacknowledged changes.
        public final AtomicLong inFlightResponses = new AtomicLong();
        public final AtomicLong inFlightBytes = new AtomicLong();
        public final AtomicLong reservedResponses = new AtomicLong();
        public final AtomicLong reservedBytes = new AtomicLong();
        public final AtomicLong maxObservedInFlightResponses = new AtomicLong();
        public final AtomicLong maxObservedInFlightBytes = new AtomicLong();
        public final AtomicLong queuedResponses = new AtomicLong();
        public final AtomicLong pendingBatches = new AtomicLong();
        public final AtomicLong retainedBatchEntries = new AtomicLong();
        public final AtomicLong maxObservedRetainedBatchEntries = new AtomicLong();
        public final AtomicLong ackNanos = new AtomicLong();
        public final AtomicLong ackedBatches = new AtomicLong();
        public final AtomicLong publisherNanos = new AtomicLong();
    }

    /**
     * Runs until EOF, failure, schema control or interruption. Always cancels its sole RPC and
     * waits for its terminal callback before returning. A retry belongs to the caller: first close
     * the old publisher, then reopen from the last durable checkpoint. Delivery is at least once.
     */
    public static void run(WatchPermissionSetsServiceGrpc.WatchPermissionSetsServiceStub stub,
                           WatchPermissionSetsRequest request, Publisher publisher,
                           CheckpointStore store, Config config, Metrics metrics) throws Exception {
        Objects.requireNonNull(stub);
        Objects.requireNonNull(request);
        var state = new State(publisher, store, config, metrics);
        Throwable thrown = null;
        try {
            stub.withMaxInboundMessageSize(config.maxResponseBytes()).watchPermissionSets(request, state);
            state.consume();
        } catch (Throwable failure) {
            thrown = failure;
            rethrow(failure);
        } finally {
            try {
                state.close();
            } catch (Throwable closing) {
                if (thrown != null) thrown.addSuppressed(closing);
                else rethrow(closing);
            }
        }
    }

    private static final class State implements
            ClientResponseObserver<WatchPermissionSetsRequest, WatchPermissionSetsResponse> {
        final Publisher publisher;
        final CheckpointStore store;
        final Config config;
        final Metrics metrics;
        final Thread consumer = Thread.currentThread();
        final Ring ring;
        final int requestChunk;
        final CountDownLatch terminated = new CountDownLatch(1);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final ArrayDeque<Pending> pending = new ArrayDeque<>();
        final ArrayDeque<Fence> checkpoints = new ArrayDeque<>();
        ArrayList<PermissionSetChange> batch;
        long batchBytes;
        long batchStarted;
        long lastSubmittedBatch;
        long acknowledgedBatchPrefix;
        volatile ClientCallStreamObserver<WatchPermissionSetsRequest> call;
        volatile boolean ended;
        volatile boolean waiting;
        Throwable sourceFailure;

        State(Publisher publisher, CheckpointStore store, Config config, Metrics metrics) {
            this.publisher = Objects.requireNonNull(publisher);
            this.store = Objects.requireNonNull(store);
            this.config = Objects.requireNonNull(config);
            this.metrics = Objects.requireNonNull(metrics);
            ring = new Ring(config.maxInFlightResponses());
            requestChunk = (int) Math.min(config.demandBatch(), Math.min(config.maxInFlightResponses(),
                    config.maxInFlightBytes() / config.maxResponseBytes()));
            batch = new ArrayList<>(Math.min(config.publishBatch(), config.maxInFlightResponses()));
        }

        @Override public void beforeStart(ClientCallStreamObserver<WatchPermissionSetsRequest> requestStream) {
            // grpc-java 1.82.2 supports zero initial requests. Every later request is explicitly budgeted.
            requestStream.disableAutoRequestWithInitial(0);
            call = requestStream;
        }

        @Override public void onNext(WatchPermissionSetsResponse response) {
            try {
                int bytes = response.getSerializedSize();
                if (bytes > config.maxResponseBytes()) {
                    fail(new IOException("Watch response exceeds configured maximum"));
                    return;
                }
                if (metrics.reservedResponses.decrementAndGet() < 0) {
                    fail(new IllegalStateException("Received an unrequested response"));
                    return;
                }
                metrics.reservedBytes.addAndGet(-config.maxResponseBytes());
                // The reserved maximum becomes actual retained serialized bytes.
                metrics.inFlightBytes.addAndGet((long) bytes - config.maxResponseBytes());
                metrics.responsesReceived.incrementAndGet();
                metrics.payloadBytesReceived.addAndGet(bytes);
                if (response.hasChange()) metrics.changesReceived.incrementAndGet();
                metrics.queuedResponses.incrementAndGet();
                if (!ring.offer(response, bytes)) {
                    metrics.queuedResponses.decrementAndGet();
                    release(1, bytes);
                    fail(new IllegalStateException("Bounded response ring overflow"));
                }
                signal();
            } catch (Throwable callbackFailure) {
                fail(callbackFailure);
            }
        }

        @Override public void onError(Throwable error) {
            sourceFailure = error;
            end();
        }

        @Override public void onCompleted() {
            sourceFailure = new IOException("Watch stream ended; resume from the durable completed revision");
            end();
        }

        void end() {
            ended = true;
            terminated.countDown();
            signal();
        }

        void consume() throws Exception {
            while (true) {
                check();
                saveReadyCheckpoints();
                requestMore();
                WatchPermissionSetsResponse response = ring.peek();
                if (response == null) {
                    // Coalesce briefly rather than creating one downstream task for each empty-ring
                    // observation. Never wait for a full batch when it exhausts demand/byte credits.
                    if (!batch.isEmpty() && (ended || System.nanoTime() - batchStarted >= MAX_BATCH_WAIT_NANOS
                            || (metrics.reservedResponses.get() == 0 && !canRequest()))) flush();
                    if (ended) {
                        // onNext may have filled the ring between peek() and the terminal callback.
                        // Observing ended publishes all preceding callback writes; check the ring again.
                        if (!ring.isEmpty()) continue;
                        flush();
                        awaitAcknowledgments();
                        rethrow(sourceFailure);
                    }
                    idle(false);
                    continue;
                }
                int bytes = ring.peekBytes();
                ring.remove();
                metrics.queuedResponses.decrementAndGet();
                switch (response.getResponseCase()) {
                    case CHANGE -> {
                        if (batch.isEmpty()) batchStarted = System.nanoTime();
                        batch.add(response.getChange());
                        batchBytes += bytes;
                        if (batch.size() >= config.publishBatch()
                                || System.nanoTime() - batchStarted >= MAX_BATCH_WAIT_NANOS) flush();
                    }
                    case COMPLETED_REVISION -> {
                        try {
                            flush();
                        } catch (Throwable failure) {
                            release(1, bytes);
                            rethrow(failure);
                        }
                        // A fence retains its control-message credit until durability, but does not
                        // stop later revisions being published while earlier ACKs are in flight.
                        checkpoints.addLast(new Fence(response.getCompletedRevision(), lastSubmittedBatch,
                                metrics.changesPublished.get(), bytes));
                        metrics.pendingCheckpoints.incrementAndGet();
                    }
                    case BREAKING_SCHEMA_CHANGE -> {
                        // API contract: a warning that source snapshot rebuilding will cause delay.
                        // Continue this stream until LookupPermissionSetsRequired supplies the rebuild boundary.
                        metrics.breakingSchemaChanges.incrementAndGet();
                        release(1, bytes);
                    }
                    case LOOKUP_PERMISSION_SETS_REQUIRED -> {
                        try {
                            flush();
                            awaitAcknowledgments();
                            throw new RebuildRequiredException(response);
                        } finally {
                            release(1, bytes);
                        }
                    }
                    case RESPONSE_NOT_SET -> {
                        release(1, bytes);
                        throw new IOException("Unknown or unset Watch response");
                    }
                }
            }
        }

        void requestMore() {
            if (ended || failure.get() != null) return;
            long outstanding = metrics.reservedResponses.get();
            if (outstanding > requestChunk / 2) return;
            long countHeadroom = config.maxInFlightResponses() - metrics.inFlightResponses.get();
            long byteHeadroom = (config.maxInFlightBytes() - metrics.inFlightBytes.get())
                    / config.maxResponseBytes();
            int count = (int) Math.min(requestChunk, Math.min(countHeadroom, byteHeadroom));
            if (count <= 0) return;
            // Prefer whole chunks; a partial request is necessary when no credits remain, otherwise
            // a tiny byte/count budget could deadlock before reaching a checkpoint or full batch.
            if (count < requestChunk && outstanding != 0) return;
            long bytes = (long) count * config.maxResponseBytes();
            // Only this thread reserves. Other threads can only free budget, so this never overshoots.
            long responses = metrics.inFlightResponses.addAndGet(count);
            long totalBytes = metrics.inFlightBytes.addAndGet(bytes);
            metrics.reservedResponses.addAndGet(count);
            metrics.reservedBytes.addAndGet(bytes);
            metrics.maxObservedInFlightResponses.accumulateAndGet(responses, Math::max);
            metrics.maxObservedInFlightBytes.accumulateAndGet(totalBytes, Math::max);
            call.request(count);
        }

        void flush() throws Exception {
            if (batch.isEmpty()) return;
            check();
            // Later batches may ACK while the oldest stalls. They release source payload credits,
            // but their prefix-tracking metadata must remain independently bounded.
            while (pending.size() >= config.maxInFlightResponses()) {
                saveReadyCheckpoints();
                idle(true);
                check();
            }
            var changes = List.copyOf(batch); // One array/list per batch, no handoff object per event.
            var work = new Pending(++lastSubmittedBatch, changes.size(), batchBytes);
            batch.clear();
            batchBytes = 0;
            pending.addLast(work);
            metrics.retainedBatchEntries.set(pending.size());
            metrics.maxObservedRetainedBatchEntries.accumulateAndGet(pending.size(), Math::max);
            metrics.pendingBatches.incrementAndGet();
            metrics.changesPublished.addAndGet(changes.size());
            long start = System.nanoTime();
            try {
                publisher.publishBatch(changes, work::complete);
            } catch (Throwable publishFailure) {
                work.complete(publishFailure);
                rethrow(publishFailure);
            } finally {
                metrics.publisherNanos.addAndGet(System.nanoTime() - start);
            }
        }

        void awaitAcknowledgments() throws Exception {
            while (true) {
                check();
                saveReadyCheckpoints();
                if (metrics.pendingBatches.get() == 0) {
                    check();
                    saveReadyCheckpoints();
                    return;
                }
                idle(true);
            }
        }

        void check() throws Exception {
            if (Thread.currentThread().isInterrupted()) throw new InterruptedException("Watch interrupted");
            Throwable problem = failure.get();
            if (problem != null) rethrow(problem);
            boolean retired = false;
            while (!pending.isEmpty() && pending.peekFirst().done) {
                acknowledgedBatchPrefix = pending.removeFirst().sequence;
                retired = true;
            }
            if (retired) metrics.retainedBatchEntries.set(pending.size());
            // terminal done is published only after any failure, so prefix retirement cannot
            // race a failure callback and incorrectly authorize a durable checkpoint.
            problem = failure.get();
            if (problem != null) rethrow(problem);
            if (!pending.isEmpty()
                    && System.nanoTime() - pending.peekFirst().started
                    >= TimeUnit.MILLISECONDS.toNanos(config.ackTimeoutMillis())) {
                throw new TimeoutException("Downstream batch acknowledgment timed out");
            }
        }

        void saveReadyCheckpoints() throws Exception {
            while (!checkpoints.isEmpty()
                    && checkpoints.peekFirst().lastBatch <= acknowledgedBatchPrefix) {
                check();
                var fence = checkpoints.peekFirst();
                store.save(fence.revision);
                metrics.changesCheckpointed.set(fence.changes);
                metrics.completedRevisions.incrementAndGet();
                checkpoints.removeFirst();
                metrics.pendingCheckpoints.decrementAndGet();
                release(1, fence.bytes);
            }
        }

        void idle(boolean barrier) {
            // Avoid a syscall/unpark for each response while the publishing thread is already busy.
            waiting = true;
            try {
                boolean ready = failure.get() != null || Thread.currentThread().isInterrupted();
                if (barrier) ready |= metrics.pendingBatches.get() == 0;
                else ready |= !ring.isEmpty() || ended || canRequest();
                long pause = TimeUnit.MILLISECONDS.toNanos(1);
                if (!barrier && !batch.isEmpty()) {
                    pause = Math.min(pause, Math.max(1, MAX_BATCH_WAIT_NANOS - (System.nanoTime() - batchStarted)));
                }
                if (!ready) LockSupport.parkNanos(this, pause);
            } finally {
                waiting = false;
            }
        }

        boolean canRequest() {
            long outstanding = metrics.reservedResponses.get();
            if (outstanding > requestChunk / 2) return false;
            long available = Math.min(config.maxInFlightResponses() - metrics.inFlightResponses.get(),
                    (config.maxInFlightBytes() - metrics.inFlightBytes.get()) / config.maxResponseBytes());
            return available >= requestChunk || (outstanding == 0 && available > 0);
        }

        void signal() { if (waiting) LockSupport.unpark(consumer); }

        void fail(Throwable problem) {
            if (failure.compareAndSet(null, problem)) {
                var stream = call;
                if (stream != null) stream.cancel("Materialize consumer failed", problem);
            }
            signal();
        }

        void release(int count, long bytes) {
            metrics.inFlightResponses.addAndGet(-count);
            metrics.inFlightBytes.addAndGet(-bytes);
        }

        void close() throws Exception {
            if (call == null) return;
            call.cancel("Materialize consumer stopped", null);
            boolean interrupted = Thread.interrupted();
            try {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (terminated.getCount() != 0 && System.nanoTime() < deadline) {
                    try {
                        terminated.await(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                    } catch (InterruptedException ignored) {
                        interrupted = true;
                    }
                }
                if (terminated.getCount() != 0) {
                    throw new IllegalStateException("RPC cancellation did not terminate callbacks; do not restart concurrently");
                }
                // No more onNext callbacks; release abandoned queued work and unused demand credits.
                while (!ring.isEmpty()) {
                    int bytes = ring.peekBytes();
                    ring.remove();
                    metrics.queuedResponses.decrementAndGet();
                    release(1, bytes);
                }
                release(batch.size(), batchBytes);
                batch.clear();
                batchBytes = 0;
                while (!checkpoints.isEmpty()) {
                    release(1, checkpoints.removeFirst().bytes);
                    metrics.pendingCheckpoints.decrementAndGet();
                }
                long unused = metrics.reservedResponses.getAndSet(0);
                metrics.reservedBytes.addAndGet(-unused * config.maxResponseBytes());
                metrics.inFlightResponses.addAndGet(-unused);
                metrics.inFlightBytes.addAndGet(-unused * config.maxResponseBytes());
                // Outstanding completions retain their own Pending object until the sink closes.
                // Prefix metadata is no longer needed once this attempt has stopped.
                pending.clear();
                metrics.retainedBatchEntries.set(0);
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }

        final class Pending {
            final long sequence;
            final int count;
            final long bytes;
            final long started = System.nanoTime();
            final AtomicBoolean claimed = new AtomicBoolean();
            volatile boolean done;
            Pending(long sequence, int count, long bytes) {
                this.sequence = sequence;
                this.count = count;
                this.bytes = bytes;
            }
            void complete(Throwable problem) {
                if (!claimed.compareAndSet(false, true)) return;
                if (problem == null) {
                    metrics.changesAcknowledged.addAndGet(count);
                    metrics.ackNanos.addAndGet(System.nanoTime() - started);
                    metrics.ackedBatches.incrementAndGet();
                } else {
                    fail(problem);
                }
                release(count, bytes);
                done = true;
                metrics.pendingBatches.decrementAndGet();
                signal();
            }
        }

        private record Fence(ZedToken revision, long lastBatch, long changes, int bytes) {}
    }

    /** SPSC ring: volatile indices publish/retire preallocated array slots. */
    private static final class Ring {
        final WatchPermissionSetsResponse[] values;
        final int[] bytes;
        volatile long tail;
        volatile long head;
        Ring(int capacity) {
            values = new WatchPermissionSetsResponse[capacity];
            bytes = new int[capacity];
        }
        boolean offer(WatchPermissionSetsResponse response, int size) {
            long index = tail;
            if (index - head == values.length) return false;
            int slot = (int) (index % values.length);
            values[slot] = response;
            bytes[slot] = size;
            tail = index + 1;
            return true;
        }
        boolean isEmpty() { return head == tail; }
        WatchPermissionSetsResponse peek() {
            return isEmpty() ? null : values[(int) (head % values.length)];
        }
        int peekBytes() { return bytes[(int) (head % values.length)]; }
        void remove() {
            long index = head;
            values[(int) (index % values.length)] = null;
            head = index + 1;
        }
    }

    private static void rethrow(Throwable failure) throws Exception {
        if (failure instanceof Exception exception) throw exception;
        if (failure instanceof Error error) throw error;
        throw new RuntimeException(failure);
    }
}
