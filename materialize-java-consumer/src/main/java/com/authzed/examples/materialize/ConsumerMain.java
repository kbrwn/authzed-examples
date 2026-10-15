package com.authzed.examples.materialize;

import com.authzed.api.materialize.v0.PermissionSetChange;
import com.authzed.api.materialize.v0.WatchPermissionSetsRequest;
import com.authzed.api.materialize.v0.WatchPermissionSetsResponse;
import com.authzed.api.materialize.v0.WatchPermissionSetsServiceGrpc;
import com.authzed.api.v1.ZedToken;
import com.authzed.grpcutil.BearerToken;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Single-pod entry point. Built-in destinations are benchmarks, never production checkpoints. */
public final class ConsumerMain {
    private static volatile long blackhole;

    public static void main(String[] args) throws Exception {
        String mode = args.length == 0 ? "receive" : args[0];
        if (!List.of("receive", "blocking", "count", "fields", "serialize", "delayed", "sink").contains(mode)) {
            throw new IllegalArgumentException("Mode: receive | blocking | count | fields | serialize | delayed | sink");
        }
        String endpoint = env("MATERIALIZE_ENDPOINT", "localhost:50051");
        boolean plaintext = env("ALLOW_PLAINTEXT", "false").equals("true");
        String authToken = env("AUTHZED_TOKEN", "");
        if (!plaintext && authToken.isBlank()) throw new IllegalArgumentException("Set AUTHZED_TOKEN for TLS endpoint");
        String initial = env("STARTING_AFTER", "");
        boolean realSink = mode.equals("sink");
        if (!realSink && !plaintext && initial.isBlank()) throw new IllegalArgumentException("Set STARTING_AFTER explicitly for a diagnostic replay");
        String sinkClass = env("SINK_FACTORY", "");
        if (realSink && sinkClass.isBlank()) throw new IllegalArgumentException("sink mode requires SINK_FACTORY");
        String sinkId = env("SINK_ID", "");
        if (realSink && sinkId.isBlank()) throw new IllegalArgumentException("sink mode requires a stable SINK_ID for the destination and mapping");
        String sourceId = env("SOURCE_ID", "");
        if (realSink && sourceId.isBlank()) throw new IllegalArgumentException("sink mode requires SOURCE_ID identifying the Materialize instance");
        Path state = Path.of(env("CHECKPOINT_PATH", "state/consumer.checkpoint"));
        var config = new AsyncMaterializeConsumer.Config(integer("MAX_IN_FLIGHT_RESPONSES", 32768),
                number("MAX_IN_FLIGHT_BYTES", 64L * 1024 * 1024), integer("MAX_RESPONSE_BYTES", 16 * 1024),
                integer("DEMAND_BATCH", 1024), integer("PUBLISH_BATCH", 256), number("ACK_TIMEOUT_MS", 30000));
        int duration = integer("DURATION_SECONDS", 60);
        int retries = integer("MAX_RECONNECTS", 0);
        var owner = Thread.currentThread();
        var timer = Executors.newSingleThreadScheduledExecutor();
        var expired = new java.util.concurrent.atomic.AtomicBoolean();
        var stopped = new CountDownLatch(1);
        var shutdown = new Thread(() -> {
            owner.interrupt();
            try { stopped.await(60, TimeUnit.SECONDS); }
            catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }, "consumer-shutdown");
        Runtime.getRuntime().addShutdownHook(shutdown);
        if (duration > 0) timer.schedule(() -> { expired.set(true); owner.interrupt(); }, duration, TimeUnit.SECONDS);
        System.err.printf("mode=%s destination=%s one_stream=true max_responses=%d byte_budget=%d max_response=%d%n",
                mode, realSink ? sinkClass : "DIAGNOSTIC_NO_DURABILITY", config.maxInFlightResponses(),
                config.maxInFlightBytes(), config.maxResponseBytes());
        // All modes take a process lock. Diagnostic modes never save a checkpoint.
        try (var checkpoint = new FileCheckpointStore(state, endpoint + "\n" + sourceId + "\n" + (realSink ? sinkId : "diagnostic"))) {
            var properties = new Properties();
            String propertiesPath = env("SINK_PROPERTIES", "");
            if (!propertiesPath.isBlank()) try (var input = Files.newInputStream(Path.of(propertiesPath))) { properties.load(input); }
            int attempt = 0;
            while (!expired.get() && !owner.isInterrupted()) {
                var saved = realSink ? checkpoint.load() : null;
                if (realSink && saved == null && initial.isBlank()) {
                    throw new IllegalArgumentException("First sink run requires STARTING_AFTER from the corresponding snapshot or durable state");
                }
                var metrics = new AsyncMaterializeConsumer.Metrics();
                var processed = new AtomicLong();
                var serialized = new AtomicLong();
                var reporter = new Reporter(mode, attempt, metrics, processed, serialized);
                var schedule = timer.scheduleAtFixedRate(reporter, 5, 5, TimeUnit.SECONDS);
                var builder = NettyChannelBuilder.forTarget(endpoint);
                if (plaintext) builder.usePlaintext(); else builder.useTransportSecurity();
                // Optional measured experiment: this retains Netty automatic BDP window tuning.
                int initialWindow = integer("INITIAL_FLOW_WINDOW", 0);
                if (initialWindow > 0) builder.initialFlowControlWindow(initialWindow);
                var channel = builder.build();
                var async = WatchPermissionSetsServiceGrpc.newStub(channel);
                var blocking = WatchPermissionSetsServiceGrpc.newBlockingStub(channel);
                if (!authToken.isBlank()) {
                    async = async.withCallCredentials(new BearerToken(authToken));
                    blocking = blocking.withCallCredentials(new BearerToken(authToken));
                }
                var request = WatchPermissionSetsRequest.newBuilder();
                if (saved != null) request.setOptionalStartingAfter(saved);
                else if (!initial.isBlank()) request.setOptionalStartingAfter(ZedToken.newBuilder().setToken(initial));
                Throwable failure = null;
                var sinkStopped = new java.util.concurrent.atomic.AtomicBoolean();
                try (var sink = trackedSink(realSink
                        ? ((BatchSinkFactory) Class.forName(sinkClass).getConstructor().newInstance()).create(properties)
                        : diagnosticSink(mode, processed, serialized), sinkStopped)) {
                    if (mode.equals("receive") || mode.equals("blocking")) {
                        receiveOnly(async, blocking, request.build(), config, metrics, mode.equals("blocking"));
                    } else {
                        AsyncMaterializeConsumer.run(async, request.build(), sink, revision -> {
                            if (realSink) checkpoint.save(revision);
                        }, config, metrics);
                    }
                } catch (Throwable caught) { failure = caught; }
                finally {
                    channel.shutdownNow();
                    // No replacement stream until both RPC and sink have stopped.
                    boolean interrupted = Thread.interrupted();
                    try {
                        if (!channel.awaitTermination(10, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Old channel has not terminated; refusing to reconnect");
                        }
                    } finally { if (interrupted) owner.interrupt(); }
                    schedule.cancel(false);
                    reporter.finish();
                }
                if (expired.get() || owner.isInterrupted() || failure instanceof InterruptedException) break;
                if (failure == null) break;
                if (!sinkStopped.get() || attempt >= retries || !transientFailure(failure)) {
                    if (failure instanceof Exception exception) throw exception;
                    throw new RuntimeException(failure);
                }
                attempt++;
                // Diagnostics restart from initial token; only a real sink has a durable resume point.
                System.err.println("Reconnect " + attempt + " after prior RPC and sink terminated");
                Thread.sleep(Math.min(5000, 250L << Math.min(attempt, 4)) + java.util.concurrent.ThreadLocalRandom.current().nextInt(100));
            }
        } finally {
            timer.shutdownNow();
            stopped.countDown();
            try { Runtime.getRuntime().removeShutdownHook(shutdown); } catch (IllegalStateException ignored) {}
        }
    }

    private static BatchSinkFactory.BatchSink trackedSink(BatchSinkFactory.BatchSink delegate,
            java.util.concurrent.atomic.AtomicBoolean stopped) {
        return new BatchSinkFactory.BatchSink() {
            @Override public void publishBatch(List<PermissionSetChange> changes, AsyncMaterializeConsumer.Completion completion) throws Exception {
                delegate.publishBatch(changes, completion);
            }
            @Override public void close() throws Exception {
                delegate.close();
                stopped.set(true);
            }
        };
    }

    static boolean transientFailure(Throwable error) {
        // A suppressed cleanup failure can mean the previous RPC or destination has not stopped.
        // Do not let a retryable primary error hide that lifecycle failure and open another stream.
        if (error.getSuppressed().length != 0) return false;
        if (error instanceof IOException) return true;
        return switch (Status.fromThrowable(error).getCode()) {
            case UNAVAILABLE, DEADLINE_EXCEEDED -> true;
            default -> false;
        };
    }

    private static BatchSinkFactory.BatchSink diagnosticSink(String mode, AtomicLong processed, AtomicLong bytes) {
        var scheduler = mode.equals("delayed") ? Executors.newSingleThreadScheduledExecutor() : null;
        return new BatchSinkFactory.BatchSink() {
            long batches;
            @Override public void publishBatch(List<PermissionSetChange> changes, AsyncMaterializeConsumer.Completion done) {
                long value = 0, total = 0;
                if (!mode.equals("count")) for (var change : changes) {
                    value += change.getParentSet().getObjectId().length() + change.getOperationValue();
                    if (mode.equals("serialize") || mode.equals("delayed")) {
                        byte[] payload = change.toByteArray();
                        total += payload.length;
                        if (payload.length != 0) value += payload[0] + payload[payload.length - 1];
                    }
                }
                blackhole = value;
                bytes.addAndGet(total);
                processed.addAndGet(changes.size());
                long batch = ++batches;
                if (scheduler == null) done.complete(null);
                else scheduler.schedule(() -> done.complete(number("FAIL_AFTER_BATCHES", 0) > 0
                        && batch >= number("FAIL_AFTER_BATCHES", 0) ? new IOException("Injected sink failure") : null),
                        number("ACK_DELAY_MS", 20), TimeUnit.MILLISECONDS);
            }
            @Override public void close() throws InterruptedException {
                if (scheduler != null) {
                    scheduler.shutdown();
                    boolean interrupted = Thread.interrupted();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
                    try {
                        while (!scheduler.isTerminated() && System.nanoTime() < deadline) {
                            try { scheduler.awaitTermination(Math.max(1, deadline - System.nanoTime()), TimeUnit.NANOSECONDS); }
                            catch (InterruptedException ignored) { interrupted = true; }
                        }
                        if (!scheduler.isTerminated()) throw new IllegalStateException("Sink did not stop");
                    } finally { if (interrupted) Thread.currentThread().interrupt(); }
                }
            }
        };
    }

    /** Minimal receive baseline. Protobuf decoding has ALREADY happened before either API delivers. */
    private static void receiveOnly(WatchPermissionSetsServiceGrpc.WatchPermissionSetsServiceStub async,
                                    WatchPermissionSetsServiceGrpc.WatchPermissionSetsServiceBlockingStub blocking,
                                    WatchPermissionSetsRequest request, AsyncMaterializeConsumer.Config config,
                                    AsyncMaterializeConsumer.Metrics metrics, boolean useBlocking) throws Exception {
        try (var context = Context.current().withCancellation()) {
            if (useBlocking) {
                var iterator = context.call(() -> blocking.withMaxInboundMessageSize(config.maxResponseBytes()).watchPermissionSets(request));
                while (iterator.hasNext()) {
                    if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
                    count(iterator.next(), metrics);
                }
                throw new IOException("Watch ended");
            }
            var ended = new CountDownLatch(1);
            var error = new AtomicReference<Throwable>();
            int window = (int) Math.min(config.demandBatch(), Math.min(config.maxInFlightResponses(),
                    config.maxInFlightBytes() / config.maxResponseBytes()));
            context.run(() -> async.withMaxInboundMessageSize(config.maxResponseBytes()).watchPermissionSets(request,
                    new ClientResponseObserver<WatchPermissionSetsRequest, WatchPermissionSetsResponse>() {
                        ClientCallStreamObserver<WatchPermissionSetsRequest> call;
                        int remaining = window;
                        public void beforeStart(ClientCallStreamObserver<WatchPermissionSetsRequest> stream) {
                            call = stream;
                            stream.disableAutoRequestWithInitial(window);
                        }
                        public void onNext(WatchPermissionSetsResponse response) {
                            count(response, metrics);
                            if (--remaining == 0) { remaining = window; call.request(remaining); }
                        }
                        public void onError(Throwable failure) { error.set(failure); ended.countDown(); }
                        public void onCompleted() { error.set(new IOException("Watch ended")); ended.countDown(); }
                    }));
            try { ended.await(); }
            finally {
                context.cancel(null);
                boolean interrupted = Thread.interrupted();
                try {
                    if (!ended.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Probe RPC did not terminate");
                } finally { if (interrupted) Thread.currentThread().interrupt(); }
            }
            if (error.get() instanceof Exception exception) throw exception;
        }
    }

    private static void count(WatchPermissionSetsResponse response, AsyncMaterializeConsumer.Metrics metrics) {
        metrics.responsesReceived.incrementAndGet();
        metrics.payloadBytesReceived.addAndGet(response.getSerializedSize());
        if (response.hasChange()) metrics.changesReceived.incrementAndGet();
    }

    static String env(String name, String fallback) { String value = System.getenv(name); return value == null ? fallback : value; }
    static long number(String name, long fallback) { return Long.parseLong(env(name, Long.toString(fallback))); }
    static int integer(String name, int fallback) { return Integer.parseInt(env(name, Integer.toString(fallback))); }

    private static final class Reporter implements Runnable {
        final String mode;
        final int attempt;
        final AsyncMaterializeConsumer.Metrics metrics;
        final AtomicLong processed, serialized;
        final long start = System.nanoTime();
        long previous = start, received, published, acknowledged, payload, lastCpu, lastGc;
        final com.sun.management.OperatingSystemMXBean os =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        final com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        final Map<Long, Long> threadCpu = new HashMap<>(), threadAllocation = new HashMap<>();
        Reporter(String mode, int attempt, AsyncMaterializeConsumer.Metrics metrics,
                 AtomicLong processed, AtomicLong serialized) {
            this.mode = mode; this.attempt = attempt; this.metrics = metrics;
            this.processed = processed; this.serialized = serialized;
            lastCpu = os.getProcessCpuTime();
        }
        @Override public void run() { report("sample"); }
        void finish() { report("final"); }
        private synchronized void report(String kind) {
            long now = System.nanoTime();
            double seconds = (now - previous) / 1e9;
            // Read downstream-to-upstream so a newer checkpoint cannot be paired with an older ACK snapshot.
            // The gauges are still approximate independent snapshots, not a globally atomic transaction.
            long k = metrics.changesCheckpointed.get(), a = metrics.changesAcknowledged.get();
            long p = metrics.changesPublished.get(), r = metrics.changesReceived.get();
            long b = metrics.payloadBytesReceived.get(), cpu = os.getProcessCpuTime();
            long heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            long direct = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                    .filter(pool -> pool.getName().equals("direct")).mapToLong(BufferPoolMXBean::getMemoryUsed).sum();
            long gc = ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(bean -> Math.max(0, bean.getCollectionTime())).sum();
            long allocated = 0;
            var hot = new java.util.ArrayList<String>();
            for (long id : threads.getAllThreadIds()) {
                long c = threads.getThreadCpuTime(id), allocation = threads.getThreadAllocatedBytes(id);
                Long old = threadCpu.put(id, c), oldAllocation = threadAllocation.put(id, allocation);
                if (oldAllocation != null && allocation >= oldAllocation) allocated += allocation - oldAllocation;
                if (old != null && c > old && (c - old) / 1e9 / seconds > 0.05) {
                    var info = threads.getThreadInfo(id);
                    if (info != null) hot.add(info.getThreadName().replace('"', '_') + ":" + String.format(Locale.ROOT, "%.2f", (c - old) / 1e9 / seconds));
                }
            }
            System.out.printf(Locale.ROOT,
                    "{\"kind\":\"%s\",\"mode\":\"%s\",\"attempt\":%d,\"elapsed_s\":%.3f,\"received_s\":%.0f,\"published_s\":%.0f,\"acked_s\":%.0f,"
                    + "\"payload_bytes_s\":%.0f,\"received\":%d,\"responses\":%d,\"processed\":%d,\"acked\":%d,\"checkpointed\":%d,\"completions\":%d,"
                    + "\"serialized_bytes\":%d,\"cpu_cores\":%.3f,\"allocated_bytes_s\":%.0f,\"heap_bytes\":%d,\"direct_bytes\":%d,\"gc_ms\":%d,"
                    + "\"queue\":%d,\"in_flight_responses\":%d,\"in_flight_bytes\":%d,\"reserved_responses\":%d,\"pending_batches\":%d,\"mean_ack_ms\":%.3f,\"hot_threads\":\"%s\"}%n",
                    kind, mode, attempt, (now - start) / 1e9, (r - received) / seconds, (p - published) / seconds, (a - acknowledged) / seconds,
                    (b - payload) / seconds, r, metrics.responsesReceived.get(), mode.equals("sink") ? p : processed.get(), a, k, metrics.completedRevisions.get(),
                    serialized.get(), (cpu - lastCpu) / 1e9 / seconds, allocated / seconds, heap, direct, gc - lastGc,
                    metrics.queuedResponses.get(), metrics.inFlightResponses.get(), metrics.inFlightBytes.get(), metrics.reservedResponses.get(),
                    metrics.pendingBatches.get(), metrics.ackedBatches.get() == 0 ? 0 : metrics.ackNanos.get() / 1e6 / metrics.ackedBatches.get(), String.join(";", hot));
            previous = now; received = r; published = p; acknowledged = a; payload = b; lastCpu = cpu; lastGc = gc;
        }
    }
}
