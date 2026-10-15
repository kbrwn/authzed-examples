package com.authzed.examples.materialize;

import com.authzed.api.materialize.v0.MemberReference;
import com.authzed.api.materialize.v0.PermissionSetChange;
import com.authzed.api.materialize.v0.SetReference;
import com.authzed.api.materialize.v0.WatchPermissionSetsRequest;
import com.authzed.api.materialize.v0.WatchPermissionSetsResponse;
import com.authzed.api.materialize.v0.WatchPermissionSetsServiceGrpc;
import com.authzed.api.v1.ZedToken;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local-only, synthetic Materialize fixture over real Netty HTTP/2, without TLS.
 * Its source data and revision tokens are NOT production Materialize data or tokens.
 * A change is one logical event and one response; completion markers are responses only.
 */
public final class FixtureServer implements AutoCloseable {
    private static final String TOKEN_PREFIX = "fixture-revision-";
    private static final int DRAIN_QUANTUM = 1024;

    public record Config(int port, int eventsPerRevision, int payloadBytes, int templates,
                         long eventsPerSecond, long burstEveryEvents, long burstPauseMillis,
                         long disconnectAfterEvents, long maxEvents) {
        public Config {
            if (port < 0 || port > 65535 || eventsPerRevision < 1 || payloadBytes < 1
                    || templates < 1 || templates > 65536 || eventsPerSecond < 0
                    || burstEveryEvents < 0 || burstPauseMillis < 0
                    || disconnectAfterEvents < 0 || maxEvents < 0) {
                throw new IllegalArgumentException("invalid fixture configuration");
            }
        }

        public static Config fromEnvironment() {
            return new Config((int) env("PORT", 50051), (int) env("EVENTS_PER_REVISION", 10000),
                    (int) env("FIXTURE_PAYLOAD_BYTES", 256), (int) env("FIXTURE_TEMPLATES", 8192),
                    env("EVENTS_PER_SECOND", 0), env("BURST_EVERY_EVENTS", 0),
                    env("BURST_PAUSE_MILLIS", 0), env("DISCONNECT_AFTER_EVENTS", 0),
                    env("FIXTURE_MAX_EVENTS", 0));
        }

        private static long env(String name, long fallback) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? fallback : Long.parseLong(value);
        }
    }

    private final Config config;
    private final ScheduledExecutorService emitter = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread thread = new Thread(r, "fixture-emitter");
        thread.setDaemon(true);
        return thread;
    });
    private final AtomicBoolean activeWatch = new AtomicBoolean();
    private final AtomicLong eventsSent = new AtomicLong();
    private final AtomicLong responsesSent = new AtomicLong();
    private final AtomicLong payloadBytesSent = new AtomicLong();
    private final PermissionSetChange[] templates;
    private final Server server;

    public FixtureServer(Config config) throws IOException {
        this.config = config;
        templates = buildTemplates(config);
        server = NettyServerBuilder.forPort(config.port()).addService(new Service()).build().start();
    }

    public int port() { return server.getPort(); }
    public boolean hasActiveWatcher() { return activeWatch.get(); }
    public long eventsSent() { return eventsSent.get(); }

    public static String revisionToken(long revision) {
        if (revision < 0) throw new IllegalArgumentException("negative fixture revision");
        return TOKEN_PREFIX + String.format(Locale.ROOT, "%020d", revision);
    }

    private static long startingRevision(WatchPermissionSetsRequest request) {
        if (!request.hasOptionalStartingAfter() || request.getOptionalStartingAfter().getToken().isEmpty()) {
            return 1;
        }
        String token = request.getOptionalStartingAfter().getToken();
        if (!token.startsWith(TOKEN_PREFIX)) throw new IllegalArgumentException("not a fixture token");
        long previous = Long.parseLong(token.substring(TOKEN_PREFIX.length()));
        if (previous < 0 || previous == Long.MAX_VALUE) throw new IllegalArgumentException("invalid revision");
        return previous + 1;
    }

    private final class Service extends WatchPermissionSetsServiceGrpc.WatchPermissionSetsServiceImplBase {
        @Override
        public void watchPermissionSets(WatchPermissionSetsRequest request,
                                        StreamObserver<WatchPermissionSetsResponse> observer) {
            final long startingRevision;
            try {
                startingRevision = startingRevision(request);
            } catch (IllegalArgumentException invalid) {
                observer.onError(Status.INVALID_ARGUMENT.withDescription(invalid.getMessage()).asRuntimeException());
                return;
            }
            if (!activeWatch.compareAndSet(false, true)) {
                observer.onError(Status.RESOURCE_EXHAUSTED.withDescription("fixture allows one active watcher")
                        .asRuntimeException());
                return;
            }
            @SuppressWarnings("unchecked")
            ServerCallStreamObserver<WatchPermissionSetsResponse> call =
                    (ServerCallStreamObserver<WatchPermissionSetsResponse>) observer;
            Stream stream = new Stream(call, startingRevision);
            call.setOnCancelHandler(stream::cancel);
            call.setOnReadyHandler(() -> stream.schedule(0));
            stream.schedule(0);
        }
    }

    /** One bounded send task per stream; no queue of generated responses. */
    private final class Stream {
        private final ServerCallStreamObserver<WatchPermissionSetsResponse> call;
        private final AtomicBoolean stopped = new AtomicBoolean();
        private final AtomicBoolean scheduled = new AtomicBoolean();
        private final long startNanos = System.nanoTime();
        private long revision;
        private long sent;
        private long notBeforeNanos;
        private int offset;
        private WatchPermissionSetsResponse[] revisionChanges;
        private WatchPermissionSetsResponse completion;

        Stream(ServerCallStreamObserver<WatchPermissionSetsResponse> call, long revision) {
            this.call = call;
            this.revision = revision;
        }

        void schedule(long delayNanos) {
            if (!stopped.get() && scheduled.compareAndSet(false, true)) {
                try {
                    emitter.schedule(this::drain, Math.max(0, delayNanos), TimeUnit.NANOSECONDS);
                } catch (java.util.concurrent.RejectedExecutionException closing) {
                    cancel();
                }
            }
        }

        void cancel() {
            if (stopped.compareAndSet(false, true)) activeWatch.set(false);
        }

        void fail(Status status) {
            if (stopped.compareAndSet(false, true)) {
                try { call.onError(status.asRuntimeException()); }
                finally { activeWatch.set(false); }
            }
        }

        private void prepareRevision() {
            ZedToken token = ZedToken.newBuilder().setToken(revisionToken(revision)).build();
            revisionChanges = new WatchPermissionSetsResponse[templates.length];
            for (int i = 0; i < templates.length; i++) {
                PermissionSetChange change = templates[i].toBuilder().setAtRevision(token)
                        .setOperation((revision & 1) == 1
                                ? PermissionSetChange.SetOperation.SET_OPERATION_ADDED
                                : PermissionSetChange.SetOperation.SET_OPERATION_REMOVED).build();
                // Cache protobuf size before the hot send path (serialization still occurs at gRPC).
                change.getSerializedSize();
                revisionChanges[i] = WatchPermissionSetsResponse.newBuilder().setChange(change).build();
            }
            completion = WatchPermissionSetsResponse.newBuilder().setCompletedRevision(token).build();
        }

        private void drain() {
            long delay = 0;
            try {
                if (stopped.get() || call.isCancelled()) { cancel(); return; }
                for (int quantum = 0; quantum < DRAIN_QUANTUM && call.isReady() && !stopped.get(); quantum++) {
                    long now = System.nanoTime();
                    if (now < notBeforeNanos) { delay = notBeforeNanos - now; break; }
                    if (config.eventsPerSecond() > 0) {
                        // Up to 1 ms of burst allowance avoids one scheduler task per event.
                        long allowance = Math.max(1, config.eventsPerSecond() / 1000);
                        double due = (double) (now - startNanos) * config.eventsPerSecond() / 1_000_000_000d;
                        if (sent >= due + allowance) { delay = 1_000_000; break; }
                    }
                    if (revisionChanges == null) prepareRevision();
                    if (offset == config.eventsPerRevision()) {
                        call.onNext(completion);
                        responsesSent.incrementAndGet();
                        offset = 0;
                        revision++;
                        revisionChanges = null;
                        continue;
                    }
                    if (config.disconnectAfterEvents() > 0 && sent >= config.disconnectAfterEvents()) {
                        fail(Status.UNAVAILABLE.withDescription("injected fixture disconnect"));
                        return;
                    }
                    if (config.maxEvents() > 0 && sent >= config.maxEvents()) {
                        if (stopped.compareAndSet(false, true)) {
                            try { call.onCompleted(); } finally { activeWatch.set(false); }
                        }
                        return;
                    }
                    WatchPermissionSetsResponse response = revisionChanges[offset % revisionChanges.length];
                    call.onNext(response);
                    offset++;
                    sent++;
                    eventsSent.incrementAndGet();
                    responsesSent.incrementAndGet();
                    payloadBytesSent.addAndGet(response.getChange().getSerializedSize());
                    if (config.burstEveryEvents() > 0 && sent % config.burstEveryEvents() == 0) {
                        notBeforeNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.burstPauseMillis());
                    }
                }
            } catch (RuntimeException error) {
                fail(Status.INTERNAL.withDescription("fixture failed").withCause(error));
            } finally {
                scheduled.set(false);
                // Recheck readiness after clearing scheduled to avoid a lost onReady notification.
                if (!stopped.get() && call.isReady()) schedule(delay);
            }
        }
    }

    private static PermissionSetChange[] buildTemplates(Config config) {
        int count = Math.min(config.templates(), config.eventsPerRevision());
        PermissionSetChange[] result = new PermissionSetChange[count];
        for (int i = 0; i < count; i++) {
            String id = String.format(Locale.ROOT, "edge-%08d", i);
            PermissionSetChange.Builder change = PermissionSetChange.newBuilder()
                    .setAtRevision(ZedToken.newBuilder().setToken(revisionToken(1)))
                    .setOperation(PermissionSetChange.SetOperation.SET_OPERATION_ADDED)
                    .setParentSet(SetReference.newBuilder().setObjectType("document").setObjectId(id)
                            .setPermissionOrRelation("view"));
            if ((i & 7) == 0) {
                change.setChildSet(SetReference.newBuilder().setObjectType("group").setObjectId(id)
                        .setPermissionOrRelation("member"));
            } else {
                change.setChildMember(MemberReference.newBuilder().setObjectType("user").setObjectId(id));
            }
            // Padding is actual protobuf string data, not transport padding or an invented proto field.
            int padding = Math.max(0, config.payloadBytes() - change.build().getSerializedSize());
            while (true) {
                change.getParentSetBuilder().setObjectId(id + "x".repeat(padding));
                int actual = change.build().getSerializedSize();
                int corrected = Math.max(0, padding + config.payloadBytes() - actual);
                if (corrected == padding) break;
                // Length-varint transitions can skip a byte; accept the closest size in that case.
                if (Math.abs(corrected - padding) == 1 && actual < config.payloadBytes()) break;
                padding = corrected;
            }
            result[i] = change.build();
        }
        return result;
    }

    public void printDescription() {
        int min = Integer.MAX_VALUE, max = 0;
        long total = 0;
        for (PermissionSetChange template : templates) {
            int size = template.getSerializedSize();
            min = Math.min(min, size); max = Math.max(max, size); total += size;
        }
        System.out.printf(Locale.ROOT,
                "fixture port=%d seed=deterministic-v1 max_watchers=1 templates=%d events_per_revision=%d "
                        + "target_events_per_second=%d change_bytes_min=%d change_bytes_mean=%.2f change_bytes_max=%d "
                        + "tls=false synthetic=true%n", port(), templates.length, config.eventsPerRevision(),
                config.eventsPerSecond(), min, (double) total / templates.length, max);
    }

    public void startReporting() {
        emitter.scheduleAtFixedRate(new Runnable() {
            private long lastEvents;
            private long lastBytes;
            private long lastNanos = System.nanoTime();
            @Override public void run() {
                long now = System.nanoTime(), events = eventsSent.get(), bytes = payloadBytesSent.get();
                double seconds = (now - lastNanos) / 1_000_000_000d;
                System.out.printf(Locale.ROOT,
                        "fixture events_sent=%d responses_sent=%d events_per_second=%.0f payload_bytes_per_second=%.0f active=%s%n",
                        events, responsesSent.get(), (events - lastEvents) / seconds,
                        (bytes - lastBytes) / seconds, activeWatch.get());
                lastEvents = events; lastBytes = bytes; lastNanos = now;
            }
        }, 5, 5, TimeUnit.SECONDS);
    }

    @Override public void close() throws InterruptedException {
        server.shutdownNow();
        server.awaitTermination(5, TimeUnit.SECONDS);
        emitter.shutdownNow();
        emitter.awaitTermination(5, TimeUnit.SECONDS);
    }

    public static void main(String[] args) throws Exception {
        FixtureServer fixture = new FixtureServer(Config.fromEnvironment());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { fixture.close(); } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        }, "fixture-shutdown"));
        fixture.printDescription();
        fixture.startReporting();
        fixture.server.awaitTermination();
    }
}
