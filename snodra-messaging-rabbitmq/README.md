# snodra-messaging-rabbitmq

The RabbitMQ Streams event store of `snodra-messaging`: publish, competing subscriptions, replay, lag and a
dead-letter stream on **RabbitMQ 3.13 or later with the stream plugin** (`rabbitmq_stream`), through the stream protocol
(port 5552, 5551 with TLS) and the RabbitMQ Stream Java client (`com.rabbitmq:stream-client`). It reads and writes the same
streams as the Rust crate `snodra-messaging-rabbitmq`: the layout on the wire below is shared, rule for rule.

```kotlin
dependencies {
    implementation("com.damoreira.snodra:snodra-messaging:0.1.0-SNAPSHOT")
    implementation("com.damoreira.snodra:snodra-messaging-rabbitmq:0.1.0-SNAPSHOT")
    implementation("com.damoreira.snodra:snodra-messaging-mongo:0.1.0-SNAPSHOT") // or another message store
}
```

```yaml
snodra:
  messaging:
    rabbitmq:
      hosts: rabbitmq-0:5552,rabbitmq-1:5552
      username: orders
      password-file: /run/secrets/rabbitmq-password
      management:
        url: http://rabbitmq:15672     # enables replay (resets)
```

**The client version is pinned**: the add-on needs `com.rabbitmq:stream-client` **1.13.1 exactly** and checks it when the store
is built. Spring Boot's dependency management sets another version (1.6.0 in Boot 4.1), so a Spring Boot service sets the
property:

```kotlin
extra["rabbit-stream-client.version"] = "1.13.1" // build.gradle.kts, with io.spring.dependency-management
```

```xml
<properties><rabbit-stream-client.version>1.13.1</rabbit-stream-client.version></properties> <!-- Maven -->
```

Without it the startup fails with that line in its message.

With the Kafka add-on on the classpath too, `snodra.messaging.event-store=rabbitmq-streams` (or `kafka`) chooses; without it
the startup fails naming both.

## Create the streams first

The library never creates them. The source as a super stream, with as many partitions as the instances should share (six by
default, as Kafka's topic), and its dead-letter stream when the dead-letter mode is `event-store`:

```bash
rabbitmq-streams add_super_stream snodra.events --partitions 6
rabbitmqadmin declare queue --name snodra.events.dlt --type stream --durable true
```

`rabbitmq-streams` has no command for a plain stream on RabbitMQ 4.x: the dead-letter stream is a queue of type `stream`, declared
with `rabbitmqadmin` (v2, shipped in the `rabbitmq:4.4.0-management` image; on a node it talks to the local management API, from
elsewhere add `--host`, `--port`, `--username`, `--password`), through the management API (`PUT /api/queues/%2F/snodra.events.dlt`
with `{"durable":true,"arguments":{"x-queue-type":"stream"}}`) or by any AMQP client with `x-queue-type=stream`.

A missing super stream fails the publish (the outbox relay retries it) and the processors' subscriptions (subscribed again after
a backoff), naming the command above.

## Settings (`snodra.messaging.rabbitmq.*`)

The Rust add-on reads the same settings as `SNODRA_RABBITMQ_*`.

| Setting | Default | Meaning |
|---|---|---|
| `hosts` | `rabbitmq` | nodes to connect to first, `host` or `host:port`, comma-separated, tried in order (then each stream's leader as the broker advertises it) |
| `port` | 5552 (5551 with TLS) | the port of a host given without one |
| `virtual-host` | `/` | |
| `username` | `guest` | |
| `password` / `password-file` | `guest` | the password, or a file holding it (trimmed at both ends; a missing, unreadable, non-UTF-8 or empty file fails the startup naming the setting and the file; the file wins, as the hub token file does); never logged, and masked in the actuator's `configprops` and `env` |
| `tls` | `false` | TLS to every node, with host name verification |
| `tls-ca-file` | none | PEM CA certificates the nodes are verified against; required with TLS (the store never connects unverified) |
| `load-balancer` | `false` | the hosts are a load balancer: connect through it until it lands on the node needed (at most 32 tries) |
| `offset-store-messages` | `10000` | a processor stores its position every so many acknowledgements... |
| `offset-store-interval` | `5s` | ...or this often, whichever first, and always when a partition moves away and at shutdown (every stored position is an entry in the stream) |
| `confirm-timeout` | `30s` | the longest a publish waits for the broker's confirm; a connection that confirms nothing within it is replaced (the relay retries) |
| `management.url` | none | the management API (`http://rabbitmq:15672`, no credentials in the URL): declares `replay`; metrics collection must be on (`management.disable_stats = false`). Every request carries the user's credentials (Basic): a plain `http` URL logs a warning at startup, prefer `https` |
| `management.username` / `password` / `password-file` | the stream user's | the file wins over the password (trimmed at both ends; an empty one fails the startup); the password is masked in the actuator's output |
| `management.ca-file` | none | PEM CA certificates of an `https` management API |

The source and its dead-letter stream are the core's: `snodra.messaging.events.source` (`snodra.events`) and `<source>.dlt`.
The Rust add-on's `SNODRA_RABBITMQ_KNOWN_SUBSCRIPTIONS` belongs to its event browser and has no Kotlin counterpart.

## What it covers

- **Events go to the super stream's partitions by key** (the stream clients' hash routing), unkeyed events by their id; a key
  always reaches a processor in publish order. Publishing waits for the broker's confirm of each event (at most `confirm-timeout`, 30 s); a
  refusal for good (access refused, an event over the 1 MiB frame) gives the event up, anything else is retried by the relay.
  A source's partitions and each stream's leader are cached and looked up again every 30 s or after a failure.
- **What decides correctness is read on each stream's leader**: stored offsets (on activation, and read back after every store
  that must hold), stream statistics, the first and last chunk of events, the chunk at a time of a reset. A replica catching up
  could answer "no offset" (the whole partition again) or "empty" (a reset to latest storing 0).
- **A chunk the client cannot hand out whole** (a checksum, a codec, a listener error, which the client only logs) ends the
  processor's subscription, which subscribes again: no acknowledgement passes the gap.
- **A processor** `<service>.<processor>` is a single active consumer of that name on every partition: the broker makes one
  instance active per partition and spreads the partitions over the instances. Positions are tracked on the server under that
  name.
- **Capabilities**: `browse`, `lag` and `native-dead-letter` always; **`replay` only with the management API**
  (`management.url`, plugin `rabbitmq_stream_management`): a reset is refused while an instance reads the processor, and only
  that API can tell. The store asks it once at startup: it must answer (else the startup fails), and with its metrics off no
  `replay` is declared. Without it, a reset is answered `UNSUPPORTED`.

## The layout on the wire (shared with the Rust add-on)

These are rules both libraries follow; a program writing snodra's streams must follow them too.

| What | Rule |
|---|---|
| Source | a super stream (`snodra.events`, partitions `snodra.events-0` .. `-N`, binding keys `0` .. `N-1`), created by the infrastructure; a plain stream source (the dead letter) is one partition, segment `0` |
| Routing | murmur3 x86_32 of the routing key's UTF-8 bytes, seed 104729, as an **unsigned** number modulo the partition count, over the partitions in the broker's order (the Java client's default hash routing). Routing key = the event's key, else its id. With 6 partitions: `order-1` 0, `order-2` 2, `customer-123` 5, `order-7` 3, `""` 4 |
| Message | one AMQP 1.0 message per event, **no sub-entry batching, no compression** (the Rust client cannot read them): exactly one `data` section = the JSON payload; `message-id` = the event id (string); `content-type` = the symbol `application/json`; message annotation `x-routing-key` (symbol) = the key (string), absent without one; every header (`type`, `id`, `correlationId`, `timestamp`, `source`, `userId`, `traceparent`, and any other, a dead letter's `snodra-dlt-*`) an application property with a string value |
| Segment / position | the partition's suffix after `<source>-` (`3`); the offset in decimal; an event's time is its chunk's timestamp |
| Consumer | name `<service>.<processor>` on every partition stream; properties `single-active-consumer=true`, `name`, `super-stream=<source>` (absent on a plain stream), `snodra-member=<random id per subscription instance>` |
| Offset tracking | reference = the consumer name, per partition stream; **the stored value is the next offset to read** (the acknowledged offset + 1); nothing stored = `first`. Not the Java client's "last processed" convention: a tool assuming it shows a value one higher |
| Dead letter | the plain stream `<source>.dlt`, with what the core publishes there (the original key, payload, headers and `snodra-dlt-*`) |
| Connections | named `snodra-messaging` |

## Limits

- **Replay needs the management API**: the stream protocol lists no consumers, so without it nothing can tell whether an instance
  still reads a processor. The API is one URL: with `replay` enabled (`management.url` set) the store cannot open while that node
  is down, and the service does not start.
- **Taking a partition over.** An instance that stops or crashes hands its partitions over at once (the broker activates another
  member). After the loss of a broker **node**, a partition whose active consumer was connected to that node has no active
  consumer until the broker's `consumer_disconnected_timeout` passes (60 s by default on RabbitMQ 4.x; measured 60.1 s on a
  3-node cluster). The add-on reads each partition on its leader's node, so the loss of a leader's node always means that wait
  for the partitions it led. Publishing to those partitions resumes as soon as a new leader is elected (seconds).
- **Lag is approximate** for what a processor has not received yet: offsets also number the streams' offset-tracking entries, so
  that part may be overstated; what it received and has not acknowledged is counted one by one (exact when caught up).
- **Redelivery after a crash**: positions are stored every 10 000 acknowledgements or 5 s (and always when a partition moves
  and at shutdown), so a crash redelivers at most what was acknowledged since the last store; the processed-event markers skip
  those duplicates. There is no publish deduplication either: an event the outbox republishes after a crash is skipped the same way.
- A partition added to a super stream is read once the instances subscribe again; publishing reads the partitions again every 30 s.
- A partition holding only offset-tracking entries (a reset stored an offset where no event was ever written) reads as empty of
  events; one whose first or last chunk of events answers and the other not fails the call rather than guess.
- A periodic store still in flight when a partition moves away may land after the move's store and lower the next owner's
  position: that owner redelivers what was acknowledged in between (duplicates the processed-event markers skip), never loses one.
- Segment names (the partitions' suffixes) are at most 128 bytes; a longer one is refused.
- A reset stores each partition's offset in turn and reads them back: a connection lost half way leaves some moved (the reset
  fails; running it again is safe).
