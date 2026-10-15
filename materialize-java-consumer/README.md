# Materialize Java consumer

A Java example for consuming AuthZed Materialize `WatchPermissionSets` with **one watcher**, bounded buffering, and a destination of your choice. Uses the official [authzed-java SDK](https://github.com/authzed/authzed-java).

## Why this approach is more efficient

A loop that reads events and then writes a batch stops reading while that write runs. This example separates those activities:

```text
One async Watch stream → bounded buffer → batch publisher → your destination
                                               ↓ acknowledgments
                                         durable checkpoint
```

- **Keep receiving during writes.** The async gRPC callback puts decoded messages into a bounded buffer. Storage calls and serialization run on the publishing thread.
- **Do work in batches.** gRPC demand is replenished in chunks, and the publisher groups changes without creating a task or future for every event. Several batches can await acknowledgment at once.
- **Keep memory under control.** Response count and encoded-byte limits cover queued data, outstanding demand, and unacknowledged changes. A slow destination applies backpressure. Decoded objects, transport buffers, and destination buffers need additional memory.
- **Keep recovery correct.** Checkpoints advance only when all changes covered by a completed revision are acknowledged. Waiting for an earlier acknowledgment does not stop submission of later batches within the limits.

This improves the application data path; achievable throughput still depends on payload sizes, resources, and destination performance.

## Try it with Docker

Requires Docker with Compose. From this directory:

```sh
docker compose up --build --abort-on-container-exit --exit-code-from consumer
docker compose down
```

The build runs the tests. The demo starts a local synthetic Materialize server and **one consumer**, then exits after 30 seconds. It batches and serializes changes with simulated 50ms acknowledgments. It does not connect to AuthZed or store data in a real destination.

The consumer prints progress every five seconds: received, published, and acknowledged changes/sec; queue and in-flight sizes; memory usage; and acknowledgment latency. In the demo, acknowledgments and checkpoints are simulated.

## Run against your Materialize instance

Requires Java 17+ and Maven 3.9+. Dependencies are pinned to authzed-java 1.6.0 and grpc-java 1.82.2. The Docker demo uses Java 21.

First, build and run a receive-only diagnostic. Stop any existing watcher before connecting this example to the same destination's stream.

```sh
mvn --batch-mode --no-transfer-progress test dependency:copy-dependencies -DincludeScope=runtime

export MATERIALIZE_ENDPOINT='your-materialize-host:443'
export AUTHZED_TOKEN='your-token'           # Supply through your secret mechanism.
export STARTING_AFTER='your-revision-token'
export DURATION_SECONDS=30

java -cp 'target/classes:target/dependency/*' \
  com.authzed.examples.materialize.ConsumerMain receive
```

TLS is enabled by default. `receive` counts and discards events; it does not save a checkpoint. Use a starting token corresponding to your existing destination state or an imported snapshot, as described in [AuthZed's architecture guidance](https://authzed.com/docs/materialize/guides/recommended-architecture).

## Connect your destination

Implement [BatchSinkFactory](src/main/java/com/authzed/examples/materialize/BatchSinkFactory.java). Its `publishBatch(changes, completion)` method receives a batch of `PermissionSetChange` objects. There is no built-in database or message-broker dependency.

Your adapter must preserve the order of conflicting changes across batches, tolerate replay, and report success only after every record in the batch is durably acknowledged. On failure, finish or cancel all remaining records before reporting the batch failure. Bound the adapter's own buffers and timeouts; `close()` must finish or abort outstanding work before returning.

With the connection variables above already set, put your adapter and its dependencies on the classpath and run:

```sh
export SOURCE_ID='your-materialize-instance-id'
export SINK_ID='your-destination-and-mapping-version'
export SINK_FACTORY='your.package.DestinationFactory'
export SINK_PROPERTIES='/config/destination.properties'
export CHECKPOINT_PATH='/persistent-state/materialize.checkpoint'
export DURATION_SECONDS=0
export MAX_RECONNECTS=10

java -cp 'target/classes:target/dependency/*:adapters/*' \
  com.authzed.examples.materialize.ConsumerMain sink
```

Keep the checkpoint on persistent storage. The first run requires `STARTING_AFTER`; later runs load the saved checkpoint. Recovery is at least once: unfinished work may replay. A snapshot-rebuild signal stops the consumer with `RebuildRequiredException` for your application to handle.

## Deployment and starting limits

Run one JVM in one pod. Reconnects wait for the prior RPC and sink to stop before opening another Watch. For Kubernetes Deployments, use `replicas: 1` and strategy `Recreate` to avoid normal rolling-update overlap. The local checkpoint-file lock is not a distributed lease.

| Setting | Default | Purpose |
| --- | ---: | --- |
| `MAX_IN_FLIGHT_RESPONSES` | 32,768 | Bound retained responses and outstanding demand |
| `MAX_IN_FLIGHT_BYTES` | 67,108,864 | Bound encoded source bytes and demand reservations |
| `MAX_RESPONSE_BYTES` | 16,384 | Reject oversized responses; adjust for your payloads |
| `PUBLISH_BATCH` | 256 | Maximum changes per destination batch |
| `ACK_TIMEOUT_MS` | 30,000 | Fail when an acknowledgment takes too long |

Start with these limits and measure received, acknowledged, and checkpointed progress. Increasing a buffer can absorb bursts; sustained throughput still requires the destination to keep up.
