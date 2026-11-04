package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.hub.v1.Cancel
import io.damoreira.kraken.hub.v1.Credentials
import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.ControlReply
import io.damoreira.kraken.hub.v1.ControlRequest
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Flow
import io.damoreira.kraken.hub.v1.Heartbeat
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.InstanceStatus
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.RegisterAck
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.ReplyEnd
import io.damoreira.kraken.hub.v1.ReplyPart
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.hub.v1.RequestMode
import io.damoreira.kraken.hub.v1.ServerMessage
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.Status
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

/**
 * An in-process stand-in for the Rust kraken hub, for starter and service unit tests that must not start a container.
 *
 * It mirrors the observable protocol behaviour: the first message must be a `Register` (`FAILED_PRECONDITION`), a type
 * owned by another service is refused (`ALREADY_EXISTS`), a later `Register` replaces an instance's types, requests are
 * forwarded unchanged round-robin to instances of the owning service (`NO_HANDLER` if nobody owns the type), replies are
 * relayed by request id, `TIMEOUT` fires after the effective timeout and a late reply is dropped,
 * `HANDLER_DISCONNECTED` is sent for in-flight requests when the handler stream ends, heartbeats are echoed, empty
 * names are `INVALID_ARGUMENT` and malformed or duplicate in-flight requests are `INVALID_REQUEST`.
 *
 * Protocol v2 (the default): every accepted `Register` with `protocol_version >= 2` is answered with a `RegisterAck`
 * (written before the echo of the heartbeat that follows it) carrying [hubId], [heartbeatInterval],
 * [heartbeatTimeout], [replyTimeout] as the default, [maxTimeout] and [typeTimeouts]. The effective timeout follows the
 * hub's rule: `timeoutMs > 0` -> `min(timeoutMs, override or maxTimeout)`, `timeoutMs == 0` -> `override or
 * replyTimeout`. Set [legacy] to emulate a protocol v1 hub that never sends an ack.
 *
 * Flow control (protocol P1, capability `flow-control`): a registrant that declares the capability gets it back in the
 * ack; with `initial_permits > 0` it is permit-managed: a request is forwarded only while it has permits (one per
 * forward), otherwise queued per instance ordered by `Request.priority` (higher first) then arrival, keeping its own
 * deadline. `Permits` messages add permits and drain the queue; like kraken-hub, a session never holds more than its cap,
 * which starts at `initial_permits` and is raised (never lowered) to `Permits.grant` when a grant is above it (P3
 * multi-hub top-up). Set [permitCap] false to drop the cap (permits add up without bound) or [permitGrants] false to
 * emulate a hub that predates `Permits.grant` (the cap stays at `initial_permits`). A full queue ([queueCapacity]) answers
 * `RESOURCE_EXHAUSTED` to senders that share `flow-control` and `HANDLER_ERROR "handler busy"` to others. Set
 * [flowControl] false to emulate the P0 hub (protocol v2 without the capability: everything unlimited).
 *
 * Multi-hub status (P3, capability `instance-status`): a registrant that declares the capability gets it back in the ack
 * and its `InstanceStatus` reports are kept ([lastStatusFrom]); reports on a stream that did not negotiate it are counted
 * ([statusMessagesFrom]) and ignored. Set [instanceStatus] false to emulate the P2 hub. [pauseReading] stops reading
 * every stream (what clients write stays queued on their side), and [requestsFrom] counts requests per sender.
 *
 * Query modes (P4a, ADR 0017, capability `scatter-gather`): `shared_query_types` may be owned by many services; a type
 * registered exclusive (or as a command) by one registrant and shared by another, even of the same service, is
 * `ALREADY_EXISTS`, and one listed both ways in a single `Register` is `INVALID_ARGUMENT`. A point query on a shared type
 * round-robins over every owner's instances. A `SCATTER_GATHER` request from a session that negotiated the capability
 * sends every registered instance owning the type (sorted by service, then instance id) an ordinary unary request with
 * id `<id>#<n>`, flow control and queues applying per target; answers go back as `ReplyPart`s in arrival order (`seq`,
 * `responder_service`, `responder_instance`), then one `ReplyEnd{parts, failed, timed_out}` once every target answered,
 * after `max_results` successful parts, or at the deadline. A target whose handler stream ends fails as a part
 * (`HANDLER_DISCONNECTED`). No target: `NO_HANDLER`; a command: `INVALID_REQUEST`; a v2 sender without the capability
 * (or any other mode): `UNSUPPORTED`, a v1 sender `INVALID_REQUEST`. Set [scatterGather] false to emulate the P3 hub:
 * no capability in the ack and `shared_query_types` ignored. [inject] writes any message to an instance's stream.
 *
 * Streaming queries (P4b, capability `streaming`): a `STREAMING` request from a session that negotiated the capability
 * goes to one live instance that lists the type **and** negotiated `streaming` (round-robin per type; owners with
 * instances but none capable: `UNSUPPORTED`), as `Request{id = <id>#1, mode = STREAMING, initial_credit}` (a uint32; 0 reads
 * as 64; `max_results` is kept by the hub) through the target's permits and queue. The handler's `ReplyPart{<id>#1}` and
 * `ReplyEnd{<id>#1}` are relayed with the sender's id, `responder_service` / `responder_instance` and the handler's
 * `seq`; each part takes one credit (initial plus every sender `Flow`, relayed as `Flow{<id>#1}`). A part beyond the
 * credit: `ReplyEnd{INVALID_REQUEST}` and `Cancel{<id>#1, "part without credit"}`; a part with `error`: `ReplyEnd{error}`;
 * `max_results` parts: `ReplyEnd{parts}` and `Cancel{"max results"}`; no part within the effective timeout (from the
 * forward, reset by every part, not running while the credit is 0 and started over by the `Flow` that raises it; a
 * queue wait counts too): `ReplyEnd{TIMEOUT}` and `Cancel{"idle timeout"}`; open
 * for [maxStreamDuration]: `ReplyEnd{CANCELLED}` and a cancel; a sender `Cancel`: `Cancel{<id>#1, reason}` to the
 * handler, nothing to the sender; the handler leaving: `ReplyEnd{HANDLER_DISCONNECTED}`; the sender leaving:
 * `Cancel{"sender disconnected"}`. A unary `Reply` to the target is one part then the end. A gather ending early sends
 * `Cancel{<id>#<n>, reason}` to its forwarded targets whose session negotiated `streaming`. Client-sent `ReplyPart`,
 * `ReplyEnd`, `Flow` and `Cancel` from sessions without the capability are ignored. Set [streaming] false to emulate
 * the P4a hub (no capability in the ack, `STREAMING` is `UNSUPPORTED`); [dropParts] drops relayed parts by `seq` (the
 * real hub's full-buffer drop). [openStreams], [creditOf], [flowsFrom] and [cancelsFrom] observe it.
 *
 * Subscription queries (P4c, capability `subscription`): a `SUBSCRIPTION` request from a session that negotiated the
 * capability goes to **every** live instance that lists the type and negotiated `subscription`, as they are at the open
 * (sorted by service, then instance id; an instance registering later is never added), each getting
 * `Request{id = <id>#n, mode = SUBSCRIPTION}` (`n` from 1; `initial_credit` and `max_results` 0) written directly, without
 * a permit or a queue; one of them, chosen round-robin per type, also gets the initial request `Request{id = <id>#0, mode
 * = UNARY, timeout_ms}` through its permits and queue. Its `Reply{payload}` is relayed as `ReplyPart{<id>, seq 1}`; a
 * target's `ReplyPart{<id>#n}` is relayed in arrival order with the hub's own `seq` (2, 3, ...) once the initial part is
 * out and while the sender's credit (`initial_credit`, 0 reads as 64, plus every `Flow`) allows, otherwise buffered up
 * to [subscriptionBuffer] (one more: `ReplyEnd{RESOURCE_EXHAUSTED "subscriber buffer (N) exceeded"}` and
 * `Cancel{"subscriber buffer exceeded"}` to every target). Nothing is relayed to the targets. A target's clean `ReplyEnd`
 * removes it; the last one ends the subscription with `ReplyEnd{parts}` once the buffer is drained; an error end or
 * error part from a target ends it with that error and `Cancel{"target <instance> failed"}` to the others; the initial
 * request failing (`Reply{error}`, `TIMEOUT` after the effective timeout, its handler gone, a full queue) ends it with
 * that error and `Cancel{"initial result failed"}`; `max_results` counts updates (`Cancel{"max results"}`); a sender
 * `Cancel` goes to every target (and to `<id>#0` while still forwarded); the sender leaving: the same with `"sender
 * disconnected"`; a target leaving: silent, the last one `ReplyEnd{HANDLER_DISCONNECTED}`; [maxSubscriptionDuration]
 * (0 = unlimited, the default): `ReplyEnd{CANCELLED}` and a cancel. Gates, in order: v1 `INVALID_REQUEST`, no capability
 * `UNSUPPORTED`, a command `INVALID_REQUEST`, no owner `NO_HANDLER`, owners without a capable instance `UNSUPPORTED`
 * naming them, a duplicate id (`<id>` or `<id>#0`) `INVALID_REQUEST`, a target at [maxSubscriptionsPerInstance]
 * `RESOURCE_EXHAUSTED`. An update target's id is no pending id. Client-sent `ReplyPart`, `ReplyEnd`, `Flow` and `Cancel`
 * are read from sessions with `streaming` or `subscription`; a gather's cancel goes to either. Set [subscription] false
 * to emulate the P4b hub. [openSubscriptions], [subscriptionTargetsOf], [subscriptionCreditOf] and [bufferedUpdatesOf]
 * observe it; [dropParts] applies to relayed updates too.
 *
 * One session per instance id, like the real hub: the first `Register` of a stream whose instance id already has a
 * registered stream replaces that older stream (it ends, its in-flight requests fail with `HANDLER_DISCONNECTED`;
 * [sessionsReplacedOf] counts it). Set [replaceSessions] false to accept parallel streams of one instance id.
 *
 * Access control (P5a, ADR 0018): [auth] (default [HubAuth.OFF]) holds the mode (`off`, `permissive`, `enforce`) and the
 * token -> ACL map (the real hub keeps digests; here the plain test token is the key). In `enforce` the first `Register`
 * of a stream is judged like the real hub does and the stream is closed with `UNAUTHENTICATED` (no credentials, an empty
 * token, `jwt`, an unknown token) or `PERMISSION_DENIED` (a service name the entry does not grant, a command, query or
 * shared query outside `handle`); a later `Register` is judged for its types against the session's own entry. A request
 * whose type is outside the entry's `send` gets an immediate `Error{PERMISSION_DENIED}` in every request mode (unary,
 * scatter-gather, streaming, subscription) before anything is routed, or `UNAUTHENTICATED` when the token left [auth]
 * meanwhile (the ACL is read per request). In `permissive` every violation is recorded and allowed; a session without
 * a valid token is not judged per request. Like the real hub, a `Register` naming the instance id of a live session of another
 * service is refused with `PERMISSION_DENIED` "instance id <first 8 characters> belongs to another service" in `enforce` and
 * `permissive` (the owner is untouched; with [auth] off the older stream is replaced as usual). [authViolations] lists them all and [tokensSeenFrom] the token of every
 * `Register` received, rejected ones included.
 *
 * JWT credentials (P5b, ADR 0019): with [HubAuth.services] set (the `[[service]]` tables) a `jwt` credential whose `azp` names the
 * registering service and that has such an entry is accepted, with the entry's rights; another `azp` is `PERMISSION_DENIED`, a
 * service without an entry too, a token that is not three segments with an `azp` is `UNAUTHENTICATED`, and without `services` the
 * hub answers "jwt credentials are not configured on this hub" like a hub with no issuer. The signature and `exp` are **not**
 * checked (the double does not know the issuer's keys): the real hub image covers them. A registrant that declares
 * `credentials-refresh` gets it in the ack ([credentialsRefresh] false emulates a hub without it); a `ClientMessage.credentials`
 * is recorded ([credentialsSeenFrom]) and judged like a `Register`'s against the session's own service (an invalid one or one for
 * another service closes the stream in `enforce`); [jwtsSeenFrom] lists the JWTs of the `Register`s.
 *
 * Processor control (P6b, ADR 0023, capability `processor-control`): a registrant that declares it gets it in the ack
 * ([processorControl] false emulates a hub before P6b); [control] writes a `ControlRequest` to an instance's session that
 * negotiated it and waits for the `ControlReply` with its `control_id` from that session; every reply received is kept
 * ([controlRepliesFrom]), the unmatched ones too.
 *
 * It does not implement consistent hashing on `routing_key`, metrics, or the real hub's owner grace period and
 * heartbeat expiry.
 */
class InProcessHub(val name: String = "kraken-hub-" + UUID.randomUUID()) : AutoCloseable {

    /** Default reply timeout for requests with `timeoutMs == 0`. */
    @Volatile
    var replyTimeout: Duration = Duration.ofSeconds(5)

    /** Set false to stop echoing heartbeats (still counted), simulating a hub that silently stopped answering. */
    @Volatile
    var echoHeartbeats: Boolean = true

    /** Emulates a protocol v1 hub: no `RegisterAck`, ever. */
    @Volatile
    var legacy: Boolean = false

    /** `RegisterAck.hub_id`. */
    val hubId: String get() = name

    /** Announced in the ack; this stand-in never evicts on missed heartbeats. */
    @Volatile
    var heartbeatInterval: Duration = Duration.ofSeconds(3)

    /** Announced in the ack; this stand-in never evicts on missed heartbeats. */
    @Volatile
    var heartbeatTimeout: Duration = Duration.ofSeconds(10)

    /** Upper clamp for `Request.timeoutMs` on types without an override. */
    @Volatile
    var maxTimeout: Duration = Duration.ofSeconds(60)

    /** Per-type timeout overrides (`KRAKEN_HUB_TIMEOUT_OVERRIDES` of the real hub). */
    @Volatile
    var typeTimeouts: Map<String, Duration> = emptyMap()

    /** Set false to never send `TIMEOUT` (a hub that loses requests), so the client's local guard is what fires. */
    @Volatile
    var emitTimeouts: Boolean = true

    /** Set false to emulate the P0 hub: a v2 ack without the `flow-control` capability, every instance unlimited. */
    @Volatile
    var flowControl: Boolean = true

    /**
     * True (the default, as the real hub): a permit-managed session never holds more than its cap (`initial_permits`,
     * raised by `Permits.grant`). False lets permits add up without bound.
     */
    @Volatile
    var permitCap: Boolean = true

    /** True (the default, as the real hub): `Permits.grant` raises the cap. False emulates a hub that predates it. */
    @Volatile
    var permitGrants: Boolean = true

    /** Set false to emulate the P2 hub: no `instance-status` capability in the ack, reports ignored. */
    @Volatile
    var instanceStatus: Boolean = true

    /**
     * Set false to emulate a hub before P6a (ADR 0022): no `processors` capability in the ack. With it (the default, as the
     * P6a hub) the `processors` of a session's reports are kept with its [lastStatusFrom] like the rest of the report.
     */
    @Volatile
    var processors: Boolean = true

    /** Set false to emulate a hub before P6b (ADR 0023): no `processor-control` capability in the ack, [control] fails. */
    @Volatile
    var processorControl: Boolean = true

    /** Set false to emulate the P3 hub: no `scatter-gather` capability in the ack, `shared_query_types` ignored. */
    @Volatile
    var scatterGather: Boolean = true

    /** Set false to emulate the P4a hub: no `streaming` capability in the ack, `STREAMING` requests are `UNSUPPORTED`. */
    @Volatile
    var streaming: Boolean = true

    /** Bound on a streaming query's whole life (`KRAKEN_HUB_MAX_STREAM_DURATION`): `ReplyEnd{CANCELLED}` and a cancel. */
    @Volatile
    var maxStreamDuration: Duration = Duration.ofMinutes(10)

    /** Set false to emulate a hub without `credentials-refresh` (a pre-P5b hub): the ack never lists it. */
    @Volatile
    var credentialsRefresh: Boolean = true

    /** Set false to emulate the P4b hub: no `subscription` capability in the ack, `SUBSCRIPTION` requests are `UNSUPPORTED`. */
    @Volatile
    var subscription: Boolean = true

    /** Updates held per subscription while its sender owes credit (`KRAKEN_HUB_SUBSCRIPTION_BUFFER`); one more ends it. */
    @Volatile
    var subscriptionBuffer: Int = 256

    /** Subscription targets one handler instance serves at once (`KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE`). */
    @Volatile
    var maxSubscriptionsPerInstance: Int = 1024

    /** Bound on a subscription's whole life (`KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION`); zero (the default) is unlimited. */
    @Volatile
    var maxSubscriptionDuration: Duration = Duration.ZERO

    /** While true, no stream's next message is read (the message being handled when it is set still completes). */
    @Volatile
    var pauseReading: Boolean = false

    /**
     * True (the default, as the real hub): a new stream registering an instance id that already has a stream replaces
     * the older one. False keeps both.
     */
    @Volatile
    var replaceSessions: Boolean = true

    /**
     * Delays each `RegisterAck` (and the stream's later messages, read in order) by this much, as network latency would;
     * a replaced older stream still ends at once. Zero by default.
     */
    @Volatile
    var ackDelay: Duration = Duration.ZERO

    /**
     * Relayed stream parts with these `seq` values are dropped on the way to the sender (still counted in `ReplyEnd.parts`
     * and still taking credit), as the real hub drops a part it could not deliver within 5 s on a full sender buffer.
     */
    @Volatile
    var dropParts: Set<Long> = emptySet()

    /** Requests queued per permit-managed instance before new ones are rejected (`KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE`). */
    @Volatile
    var queueCapacity: Int = 10_000

    /**
     * True (the default, as the real hub): setting [auth] closes the live sessions the new ACL no longer allows. False keeps
     * them, to test the per-request refusal a request gets in the instant before a reload closes its stream.
     */
    @Volatile
    var closeSessionsOnAuthChange: Boolean = true

    @Volatile
    private var currentAuth: HubAuth = HubAuth.OFF

    /**
     * Access control (P5a): the mode and the token -> ACL map. Read per `Register` and per request, so it may change at
     * runtime. Like the real hub on an ACL reload, setting it re-judges every live session: in `enforce` a session whose
     * token is gone or whose registration the new ACL no longer allows is closed (`UNAUTHENTICATED` for a token that left,
     * `PERMISSION_DENIED` for a service name or a handled type it no longer grants; its in-flight requests fail as on any
     * disconnect); `permissive` and `off` keep every session.
     */
    var auth: HubAuth
        get() = currentAuth
        set(value) {
            currentAuth = value
            if (closeSessionsOnAuthChange) reevaluateSessions(value)
        }

    private class Session(val out: SendChannel<ServerMessage>) {
        @Volatile var serviceName: String = ""
        @Volatile var instanceId: String = ""
        @Volatile var types: Set<String> = emptySet()
        @Volatile var commandTypes: Set<String> = emptySet()
        @Volatile var sharedTypes: Set<String> = emptySet()
        @Volatile var protocolVersion = 0
        /** Negotiated `scatter-gather` (fixed by the stream's first Register). */
        @Volatile var scatterGather = false
        /** Negotiated `streaming` (fixed by the stream's first Register). */
        @Volatile var streaming = false
        /** Negotiated `subscription` (fixed by the stream's first Register). */
        @Volatile var subscription = false
        /** Reads the multi-part client messages (`ReplyPart`, `ReplyEnd`, `Flow`, `Cancel`) and understands `Cancel`. */
        val multiPart: Boolean get() = streaming || subscription
        @Volatile var registered = false
        /** The stream's first `Register` was judged for credentials (ruling 5 of ADR 0018: only the first counts). */
        var credentialsJudged = false
        /** The token the stream's first `Register` carried; null without a token (or with `jwt`). */
        @Volatile var token: String? = null
        /** The service a `jwt` credential named (its `azp`) and [auth] has an entry for; null for a token session. */
        @Volatile var jwtService: String? = null
        /** True once the token is one [auth] knew at the first `Register`: the session is judged per request. */
        @Volatile var authenticated = false
        /** Negotiated `credentials-refresh` (fixed by the stream's first Register). */
        @Volatile var credentialsRefresh = false
        /** Shares `flow-control` with this hub (fixed by the stream's first Register). */
        @Volatile var flowControl = false
        /** Negotiated `instance-status` (fixed by the stream's first Register). */
        @Volatile var instanceStatus = false
        @Volatile var processorControl = false
        /** The ack of the Register just handled, written by the stream's collector (after [ackDelay]). */
        var ack: ServerMessage? = null
        /** Remaining permits; null = unlimited (not permit-managed). Guarded by [lock]. */
        var permits: Int? = null
        /** Most permits the session may hold: `initial_permits`, raised by `Permits.grant`. Guarded by [lock]. */
        var cap: Int = 0
        /** Requests waiting for a permit, highest priority first, then arrival. Guarded by [lock]. */
        val queue = ArrayList<Queued>()
        lateinit var collector: Job
    }

    private class Queued(val request: Request, val seq: Long)

    /**
     * A forwarded request: a unary one with its own deadline [timer], one target of [gather], the target of [stream], or
     * the initial request (`<id>#0`) of [subscription].
     */
    private class Pending(
        val sender: Session,
        val handler: Session,
        val timer: Job?,
        val gather: Gather? = null,
        val stream: Stream? = null,
        val subscription: Subscription? = null,
    )

    /** A subscription query in progress (P4c); guarded by [lock]. */
    private class Subscription(val id: String, val sender: Session, val type: String, var credit: Long, val maxResults: Int) {
        val initialId: String get() = "$id#0"
        lateinit var initialHandler: Session
        /** The initial request left the handler's queue (a cancel goes only to a forwarded request). */
        var initialForwarded = false
        var initialDone = false
        /** The update targets still serving, by target id, in target order. */
        val targets = LinkedHashMap<String, Session>()
        /** Parts relayed (the initial part included): the hub's `seq`. */
        var parts = 0
        var updates = 0
        /** Updates waiting for the initial part or for credit, in arrival order (`seq` is set when relayed). */
        val buffer = ArrayDeque<ReplyPart.Builder>()
        var duration: Job? = null
    }

    /** A streaming query in progress (P4b); guarded by [lock]. */
    private class Stream(val id: String, val sender: Session, val handler: Session, var credit: Long, val maxResults: Int, val timeout: Duration) {
        val targetId: String get() = "$id#1"
        var parts = 0
        /** The target left the handler's queue (a cancel goes only to a forwarded target). */
        var forwarded = false
        var idle: Job? = null
        lateinit var duration: Job
    }

    /** A scatter-gather in progress; guarded by [lock]. */
    private class Gather(val id: String, val sender: Session, val maxResults: Int) {
        val remaining = LinkedHashSet<String>() // target ids that have not answered
        var seq = 0L
        var parts = 0
        var failed = 0
        var ok = 0
        lateinit var timer: Job
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val lock = Any() // guards sessions, pending and rotation; never held across a suspension
    private val sessions = CopyOnWriteArrayList<Session>()
    private val pending = HashMap<String, Pending>()
    private val gathers = HashMap<String, Gather>()
    private val streams = HashMap<String, Stream>() // by the sender's request id
    private val subscriptions = HashMap<String, Subscription>() // by the sender's request id
    private val flowsBySender = ConcurrentHashMap<String, MutableList<Int>>()
    private val cancelsBySender = ConcurrentHashMap<String, MutableList<Cancel>>()
    private val rotation = HashMap<String, Int>()
    private val heartbeats = ConcurrentHashMap<String, Int>()
    private val registrations = ConcurrentHashMap<String, Int>()
    private val lastRegisters = ConcurrentHashMap<String, Register>()
    private val acks = ConcurrentHashMap<String, Int>()
    private val permitsGranted = ConcurrentHashMap<String, Int>()
    private val permitMessages = ConcurrentHashMap<String, Int>()
    private val forwardedOrder = CopyOnWriteArrayList<String>()
    private val requestsBySender = ConcurrentHashMap<String, Int>()
    private val statusMessages = ConcurrentHashMap<String, Int>()
    private val lastStatuses = ConcurrentHashMap<String, InstanceStatus>()
    private val controlReplies = ConcurrentHashMap<String, CopyOnWriteArrayList<ControlReply>>()
    private val controlWaiters = ConcurrentHashMap<String, Pair<Session, CompletableDeferred<ControlReply>>>()
    private val streamsOpened = ConcurrentHashMap<String, Int>()
    private val streamsReplaced = ConcurrentHashMap<String, Int>()
    private val tokensSeen = ConcurrentHashMap<String, CopyOnWriteArrayList<String?>>()
    private val jwtsSeen = ConcurrentHashMap<String, CopyOnWriteArrayList<String>>()
    private val credentialsSeen = ConcurrentHashMap<String, CopyOnWriteArrayList<String>>()
    private val violations = CopyOnWriteArrayList<AuthViolation>()
    private var seq = 0L
    @Volatile private var lastRequest: Request? = null
    private val channels = CopyOnWriteArrayList<ManagedChannel>()
    @Volatile private var lastHeaders: Map<String, String>? = null
    private var server: Server? = null

    fun start(): InProcessHub {
        check(server == null) { "hub already started" }
        server = InProcessServerBuilder.forName(name).addService(Service()).build().start()
        return this
    }

    fun channel(): ManagedChannel =
        InProcessChannelBuilder.forName(name).build().also { channels += it }

    fun registeredTypes(): Set<String> = sessions.flatMapTo(HashSet()) { it.types }

    fun connectedInstances(): Set<String> = sessions.filter { it.registered }.mapTo(HashSet()) { it.instanceId }

    /** Ends the stream of [instanceId] as if its process crashed; the client sees the stream fail with UNAVAILABLE. */
    fun dropInstance(instanceId: String) = dropInstance(instanceId, Status.UNAVAILABLE.withDescription("instance $instanceId dropped by test"))

    /** Ends the stream of [instanceId] with [status], as the hub does for a refusal or a session limit (for example `UNAUTHENTICATED` "certificate expired"). */
    fun dropInstance(instanceId: String, status: Status) {
        sessions.filter { it.registered && it.instanceId == instanceId }.forEach {
            it.out.close(status.asException())
            it.collector.cancel()
        }
    }

    /** Open streams that registered as [instanceId]. */
    fun sessionsOf(instanceId: String): Int = sessions.count { it.registered && it.instanceId == instanceId }

    /** Streams whose first `Register` named [instanceId]. */
    fun streamsOpenedBy(instanceId: String): Int = streamsOpened[instanceId] ?: 0

    /** Streams of [instanceId] ended because a newer stream registered the same instance id ([replaceSessions]). */
    fun sessionsReplacedOf(instanceId: String): Int = streamsReplaced[instanceId] ?: 0

    /** `Request` messages received from [instanceId] over all of its streams. */
    fun requestsFrom(instanceId: String): Int = requestsBySender[instanceId] ?: 0

    /** `InstanceStatus` messages received from [instanceId], kept or ignored. */
    fun statusMessagesFrom(instanceId: String): Int = statusMessages[instanceId] ?: 0

    /** The latest `InstanceStatus` kept from [instanceId] (only on streams that negotiated `instance-status`), or null. */
    fun lastStatusFrom(instanceId: String): InstanceStatus? = lastStatuses[instanceId]

    fun heartbeatsFrom(instanceId: String): Int = heartbeats[instanceId] ?: 0

    /** Accepted `Register` messages from [instanceId] over all of its streams. */
    fun registrationsFrom(instanceId: String): Int = registrations[instanceId] ?: 0

    /** The most recent accepted `Register` from [instanceId], or null. */
    fun lastRegisterFrom(instanceId: String): Register? = lastRegisters[instanceId]

    /**
     * The token (null: none, or `jwt` credentials) of every `Register` [instanceId] sent, in order, including the ones this
     * hub refused; one entry per `Register`, so one per stream plus one per later registration change.
     */
    fun tokensSeenFrom(instanceId: String): List<String?> = tokensSeen[instanceId]?.toList().orEmpty()

    /** The `jwt` of every `Register` [instanceId] sent that carried one, in order (refused ones included). */
    fun jwtsSeenFrom(instanceId: String): List<String> = jwtsSeen[instanceId]?.toList().orEmpty()

    /** The `jwt` or `token` of every `ClientMessage.credentials` (credential refresh) [instanceId] sent, in order, whatever the hub did with it. */
    fun credentialsSeenFrom(instanceId: String): List<String> = credentialsSeen[instanceId]?.toList().orEmpty()

    /** Every access-control violation so far (refused in `enforce`, allowed in `permissive`), in order. */
    fun authViolations(): List<AuthViolation> = violations.toList()

    /** `RegisterAck`s sent to [instanceId] over all of its streams. */
    fun acksSentTo(instanceId: String): Int = acks[instanceId] ?: 0

    /** The most recent `Request` any client sent (as received, before routing), or null. */
    fun lastRequest(): Request? = lastRequest

    /** Sum of `Permits.count` received from [instanceId] over all of its streams. */
    fun permitsGrantedBy(instanceId: String): Int = permitsGranted[instanceId] ?: 0

    /** Number of `Permits` messages received from [instanceId] over all of its streams. */
    fun permitMessagesFrom(instanceId: String): Int = permitMessages[instanceId] ?: 0

    /** Permits [instanceId] has left on its current stream; null when it is not permit-managed (unlimited). */
    fun permitsAvailable(instanceId: String): Int? = synchronized(lock) {
        sessions.firstOrNull { it.registered && it.instanceId == instanceId }?.permits
    }

    /** Requests waiting in [instanceId]'s queue. */
    fun queuedFor(instanceId: String): Int = synchronized(lock) {
        sessions.filter { it.registered && it.instanceId == instanceId }.sumOf { it.queue.size }
    }

    /**
     * Sends [request] to the live session of [instanceId] (it must have negotiated `processor-control`) and waits up to
     * [timeout] for its `ControlReply` from that session.
     */
    suspend fun control(instanceId: String, request: ControlRequest, timeout: Duration = Duration.ofSeconds(10)): ControlReply {
        val session = sessions.lastOrNull { it.registered && it.instanceId == instanceId }
            ?: throw IllegalStateException("no live session of $instanceId")
        check(session.processorControl) { "$instanceId did not negotiate processor-control" }
        val answer = CompletableDeferred<ControlReply>()
        controlWaiters[request.controlId] = session to answer
        session.out.trySend(ServerMessage.newBuilder().setControl(request).build())
        return try {
            withTimeout(timeout.toMillis()) { answer.await() }
        } finally {
            controlWaiters.remove(request.controlId)
        }
    }

    /** Every `ControlReply` [instanceId] sent, in order. */
    fun controlRepliesFrom(instanceId: String): List<ControlReply> = controlReplies[instanceId]?.toList().orEmpty()

    /** Writes [message] to the stream of [instanceId] as if the hub had sent it (for example a part nobody waits for). */
    fun inject(instanceId: String, message: ServerMessage) {
        sessions.filter { it.registered && it.instanceId == instanceId }.forEach { it.out.trySend(message) }
    }

    /** Request ids in the order they were forwarded to handlers. */
    fun forwardedOrder(): List<String> = forwardedOrder.toList()

    /** Streaming queries not yet ended. */
    fun openStreams(): Int = synchronized(lock) { streams.size }

    /** The credit the hub still holds for the stream with the sender's request id [requestId], or null when it is not open. */
    fun creditOf(requestId: String): Long? = synchronized(lock) { streams[requestId]?.credit }

    /** `Flow.credits` received from [instanceId] as a sender, in order (ignored ones included). */
    fun flowsFrom(instanceId: String): List<Int> = flowsBySender[instanceId]?.toList().orEmpty()

    /** `Cancel` messages received from [instanceId] as a sender, in order (ignored ones included). */
    fun cancelsFrom(instanceId: String): List<Cancel> = cancelsBySender[instanceId]?.toList().orEmpty()

    /** Subscription queries not yet ended. */
    fun openSubscriptions(): Int = synchronized(lock) { subscriptions.size }

    /** Subscription targets the instance [instanceId] serves (the real hub's `kraken_hub_subscriptions_open`). */
    fun subscriptionTargetsOf(instanceId: String): Int = synchronized(lock) {
        subscriptions.values.sumOf { sub -> sub.targets.values.count { it.instanceId == instanceId } }
    }

    /** The credit the hub still holds for the subscription with the sender's request id [requestId], or null when it is not open. */
    fun subscriptionCreditOf(requestId: String): Long? = synchronized(lock) { subscriptions[requestId]?.credit }

    /** Updates the hub holds for the subscription [requestId] (beyond the credit, or before its initial part); 0 when it is not open. */
    fun bufferedUpdatesOf(requestId: String): Int = synchronized(lock) { subscriptions[requestId]?.buffer?.size ?: 0 }

    /** The ack this hub sends to a v2 registrant. */
    fun registerAck(): RegisterAck = RegisterAck.newBuilder()
        .setHubId(hubId)
        .setHeartbeatIntervalMs(heartbeatInterval.toMillis().toInt())
        .setHeartbeatTimeoutMs(heartbeatTimeout.toMillis().toInt())
        .setDefaultTimeoutMs(replyTimeout.toMillis().toInt())
        .setMaxTimeoutMs(maxTimeout.toMillis().toInt())
        .setContext("default")
        .putAllTypeTimeoutsMs(typeTimeouts.mapValues { it.value.toMillis().toInt() })
        .build()

    /** Headers of the most recently forwarded request, or null if none was forwarded yet. */
    fun lastRequestHeaders(): Map<String, String>? = lastHeaders

    override fun close() {
        scope.cancel()
        channels.forEach { it.shutdownNow() }
        server?.shutdownNow()
        server?.awaitTermination(5, TimeUnit.SECONDS)
        channels.forEach { it.awaitTermination(5, TimeUnit.SECONDS) }
    }

    private inner class Service : HubGrpcKt.HubCoroutineImplBase() {
        override fun connect(requests: kotlinx.coroutines.flow.Flow<ClientMessage>): kotlinx.coroutines.flow.Flow<ServerMessage> = channelFlow {
            val session = Session(channel)
            sessions += session
            session.collector = launch {
                try {
                    var first = true
                    requests.collect { msg ->
                        if (first && !msg.hasRegister()) {
                            throw Status.FAILED_PRECONDITION.withDescription("first message must be Register").asException()
                        }
                        first = false
                        while (pauseReading) delay(5)
                        handle(session, msg)
                        session.ack?.let { ack ->
                            session.ack = null
                            if (!ackDelay.isZero) delay(ackDelay.toMillis())
                            session.out.trySend(ack)
                        }
                    }
                } finally {
                    cleanup(session)
                }
            }
        }.buffer(Channel.UNLIMITED)
    }

    private fun handle(session: Session, msg: ClientMessage) {
        when {
            msg.hasRegister() -> register(session, msg.register)
            msg.hasRequest() -> forward(session, msg.request)
            msg.hasReply() -> relay(session, msg)
            msg.hasPermits() -> grant(session, msg.permits.count, msg.permits.grant)
            msg.hasPart() -> relayPart(session, msg.part)
            msg.hasEnd() -> relayEnd(session, msg.end)
            msg.hasFlow() -> relayFlow(session, msg.flow)
            msg.hasCancel() -> onCancel(session, msg.cancel)
            msg.hasCredentials() -> refreshCredentials(session, msg.credentials)
            msg.hasControlReply() -> {
                controlReplies.computeIfAbsent(session.instanceId) { CopyOnWriteArrayList() } += msg.controlReply
                val waiter = controlWaiters[msg.controlReply.controlId]
                // Like the real hub: only the session the request went to answers it.
                if (waiter != null && waiter.first === session && controlWaiters.remove(msg.controlReply.controlId, waiter)) {
                    waiter.second.complete(msg.controlReply)
                }
            }
            msg.hasStatus() -> {
                statusMessages.merge(session.instanceId, 1, Int::plus)
                if (session.instanceStatus) lastStatuses[session.instanceId] = msg.status
            }
            msg.hasHeartbeat() -> {
                heartbeats.merge(session.instanceId, 1, Int::plus)
                if (echoHeartbeats) session.out.trySend(
                    ServerMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtMs(msg.heartbeat.sentAtMs)).build(),
                )
            }
        }
    }

    private fun register(session: Session, reg: Register) {
        if (reg.serviceName.isEmpty() || reg.instanceId.isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("service_name and instance_id must not be empty").asException()
        }
        authorizeRegister(session, reg)
        val exclusive = (reg.commandTypesList + reg.queryTypesList).toSet()
        // A P3 hub predates shared_query_types: the field is ignored (the types are simply not registered).
        val shared = if (scatterGather && !legacy) reg.sharedQueryTypesList.toSet() else emptySet()
        shared.firstOrNull { it in exclusive }?.let { type ->
            throw Status.INVALID_ARGUMENT.withDescription("type $type is listed as shared and as exclusive").asException()
        }
        val types = exclusive + shared
        val replaced = synchronized(lock) {
            // The real hub refuses (in `enforce` and `permissive`, not in `off`) a stream that names the instance id of a
            // live session of another service: the owner is untouched, nothing is replaced.
            if (!session.registered && auth.mode != AuthMode.OFF &&
                sessions.any { it !== session && it.registered && it.instanceId == reg.instanceId && it.serviceName != reg.serviceName }
            ) {
                val message = "instance id ${reg.instanceId.take(8)} belongs to another service"
                violations += AuthViolation(reg.instanceId, Status.Code.PERMISSION_DENIED, message, true, System.nanoTime())
                throw Status.PERMISSION_DENIED.withDescription(message).asException()
            }
            for (type in exclusive) {
                val owner = sessions.firstOrNull { it !== session && type in it.types && it.serviceName != reg.serviceName }
                if (owner != null) {
                    throw Status.ALREADY_EXISTS.withDescription("type $type is owned by service ${owner.serviceName}").asException()
                }
                val sharedOwner = sessions.firstOrNull { it !== session && type in it.sharedTypes }
                if (sharedOwner != null) {
                    throw Status.ALREADY_EXISTS.withDescription("type $type is shared by service ${sharedOwner.serviceName}").asException()
                }
            }
            for (type in shared) {
                val owner = sessions.firstOrNull { it !== session && type in it.types && type !in it.sharedTypes }
                if (owner != null) {
                    throw Status.ALREADY_EXISTS.withDescription("type $type is owned exclusively by service ${owner.serviceName}").asException()
                }
            }
            val first = !session.registered
            // Like the real hub: one session per instance id, the newer stream wins and the older one stops being routed to.
            val older = if (first && replaceSessions) {
                sessions.filter { it !== session && it.registered && it.instanceId == reg.instanceId }.onEach { it.registered = false }
            } else {
                emptyList()
            }
            if (first) { // like the real hub: the first Register of a stream fixes flow control
                session.flowControl = !legacy && flowControl
                session.permits = if (session.flowControl && reg.initialPermits > 0) reg.initialPermits else null
                session.cap = reg.initialPermits
                session.instanceStatus = !legacy && instanceStatus
                session.scatterGather = !legacy && scatterGather
                session.streaming = !legacy && streaming
                session.subscription = !legacy && subscription
                session.credentialsRefresh = !legacy && credentialsRefresh
                session.processorControl = !legacy && processorControl
                session.protocolVersion = if (legacy) 0 else 2
            }
            session.serviceName = reg.serviceName
            session.instanceId = reg.instanceId
            session.registered = true
            session.types = types
            session.commandTypes = reg.commandTypesList.toSet()
            session.sharedTypes = shared
            if (first) streamsOpened.merge(reg.instanceId, 1, Int::plus)
            older
        }
        // The older stream ends (its in-flight requests fail in its cleanup) before this one is acknowledged.
        for (old in replaced) {
            streamsReplaced.merge(reg.instanceId, 1, Int::plus)
            old.out.close()
            old.collector.cancel()
        }
        registrations.merge(reg.instanceId, 1, Int::plus)
        lastRegisters[reg.instanceId] = reg
        if (!legacy) {
            // Written before this stream's next message is handled, so it precedes the echo of the following heartbeat.
            session.ack = ServerMessage.newBuilder().setRegisterAck(registerAck()).build()
            acks.merge(reg.instanceId, 1, Int::plus)
        }
    }

    /** The real hub's `reevaluate_sessions`: re-judges every registered session against [acl], closing the denied ones in `enforce`. */
    private fun reevaluateSessions(acl: HubAuth) {
        if (acl.mode != AuthMode.ENFORCE) return
        for (session in sessions.filter { it.registered }) {
            val denied = recheck(session, acl) ?: continue
            violations += AuthViolation(session.instanceId, denied.first, denied.second, true, System.nanoTime())
            session.out.close(denied.first.toStatus().withDescription(denied.second).asException())
            session.collector.cancel()
        }
    }

    /** The first reason [session] is no longer allowed by [acl] (an `enforce` ACL), as the real hub's `recheck`, or null. */
    private fun recheck(session: Session, acl: HubAuth): Pair<Status.Code, String>? {
        val jwtService = session.jwtService
        val entry = if (jwtService != null) {
            acl.service(jwtService) ?: return Status.Code.PERMISSION_DENIED to "no ACL entry for service $jwtService"
        } else {
            val token = session.token ?: return Status.Code.UNAUTHENTICATED to "credentials required"
            acl.tokens[token] ?: return Status.Code.UNAUTHENTICATED to "token no longer accepted"
        }
        if (entry.service != session.serviceName) {
            return Status.Code.PERMISSION_DENIED to "token is not allowed to register as service ${session.serviceName}"
        }
        val denied = session.types.filterNot { HubAuth.matches(entry.handle, it) }
        if (denied.isEmpty()) return null
        val more = if (denied.size > 1) " (and ${denied.size - 1} more)" else ""
        return Status.Code.PERMISSION_DENIED to "service ${session.serviceName} is not allowed to handle type ${denied.first()}$more"
    }

    /** Judges a `Register` against [auth]; throws the status that closes the stream in `enforce`. */
    private fun authorizeRegister(session: Session, reg: Register) {
        val acl = auth
        session.instanceId = reg.instanceId // so a violation names the instance; only a registered session is ever looked up by it
        val token = if (reg.hasCredentials() && reg.credentials.hasToken()) reg.credentials.token else null
        val jwt = if (reg.hasCredentials() && reg.credentials.hasJwt()) reg.credentials.jwt else null
        tokensSeen.computeIfAbsent(reg.instanceId) { CopyOnWriteArrayList() }.add(token)
        if (jwt != null) jwtsSeen.computeIfAbsent(reg.instanceId) { CopyOnWriteArrayList() }.add(jwt)
        if (acl.mode == AuthMode.OFF) return
        if (!session.credentialsJudged) {
            session.credentialsJudged = true
            session.token = token
            val entry = when {
                jwt != null -> judgeJwt(acl, session, jwt, reg.serviceName) ?: return
                token.isNullOrEmpty() ->
                    return violate(acl, session, Status.Code.UNAUTHENTICATED, "no credentials: a token is required")
                else -> acl.tokens[token]
                    ?: return violate(acl, session, Status.Code.UNAUTHENTICATED, "unknown token")
            }
            session.authenticated = true
            if (reg.serviceName != entry.service) {
                violate(acl, session, Status.Code.PERMISSION_DENIED, "token may not register as service ${reg.serviceName}")
            }
        }
        val entry = entryOf(session, acl)
        if (!session.authenticated) return // permissive without a valid token: allowed, nothing more to judge
        if (entry == null) return violate(acl, session, Status.Code.UNAUTHENTICATED, "token is no longer known")
        val denied = (reg.commandTypesList + reg.queryTypesList + reg.sharedQueryTypesList).filterNot { HubAuth.matches(entry.handle, it) }
        if (denied.isNotEmpty()) {
            val more = if (denied.size > 1) " (and ${denied.size - 1} more)" else ""
            violate(acl, session, Status.Code.PERMISSION_DENIED, "token may not handle type ${denied.first()}$more")
        }
    }

    /** The ACL entry [session] is judged by: its `jwt` service's, else its token's. */
    private fun entryOf(session: Session, acl: HubAuth): AclEntry? =
        session.jwtService?.let { acl.service(it) } ?: session.token?.let { acl.tokens[it] }

    /**
     * The entry of [service] when [jwt] is a credential the real hub would accept from it, else null after [violate] (which throws
     * in `enforce`): not configured, not a token with an `azp`, another service's, or a service with no entry.
     */
    private fun judgeJwt(acl: HubAuth, session: Session, jwt: String, service: String): AclEntry? {
        if (!acl.jwtEnabled) return violate(acl, session, Status.Code.UNAUTHENTICATED, "jwt credentials are not configured on this hub").let { null }
        val claims = UnverifiedJwt.parse(jwt) ?: return violate(acl, session, Status.Code.UNAUTHENTICATED, "malformed jwt").let { null }
        if (claims.azp != service) {
            return violate(acl, session, Status.Code.PERMISSION_DENIED, "token azp ${claims.azp.take(128)} is not service ${service.take(128)}").let { null }
        }
        val entry = acl.service(claims.azp)
            ?: return violate(acl, session, Status.Code.PERMISSION_DENIED, "no ACL entry for service ${claims.azp.take(128)}").let { null }
        session.jwtService = claims.azp
        return entry
    }

    /**
     * A `ClientMessage.credentials`: recorded, then judged like a `Register`'s credential against the session's own service. A
     * valid one replaces the session's token or jwt; an invalid one, or one for another service, closes the stream in `enforce`
     * (`permissive` records it and keeps the old credential). The real hub's one-second throttle is not emulated.
     */
    private fun refreshCredentials(session: Session, credentials: Credentials) {
        val value = if (credentials.hasJwt()) credentials.jwt else credentials.token
        credentialsSeen.computeIfAbsent(session.instanceId) { CopyOnWriteArrayList() }.add(value)
        val acl = auth
        if (acl.mode == AuthMode.OFF) return
        if (credentials.hasJwt()) {
            judgeJwt(acl, session, value, session.serviceName) ?: return
            session.token = null
            session.authenticated = true
        } else {
            val entry = acl.tokens[value]
                ?: return violate(acl, session, Status.Code.UNAUTHENTICATED, "unknown token")
            if (entry.service != session.serviceName) {
                return violate(acl, session, Status.Code.PERMISSION_DENIED, "token may not register as service ${session.serviceName}")
            }
            session.token = value
            session.jwtService = null
            session.authenticated = true
        }
    }

    /** Records a violation of [session] and, in `enforce`, closes its stream with [code]. */
    private fun violate(acl: HubAuth, session: Session, code: Status.Code, message: String) {
        val enforced = acl.mode == AuthMode.ENFORCE
        violations += AuthViolation(session.instanceId, code, message, enforced, System.nanoTime())
        if (enforced) throw code.toStatus().withDescription(message).asException()
    }

    /** True when [sender] may send [req]; otherwise (`enforce`) the `Error` was written and nothing must be routed. */
    private fun maySend(sender: Session, req: Request): Boolean {
        val acl = auth
        if (acl.mode == AuthMode.OFF || !sender.authenticated) return true
        val entry = entryOf(sender, acl)
        val (code, message) = when {
            entry == null -> ErrorCode.UNAUTHENTICATED to "token is no longer known"
            !HubAuth.matches(entry.send, req.type) -> ErrorCode.PERMISSION_DENIED to "token may not send type ${req.type}"
            else -> return true
        }
        val enforced = acl.mode == AuthMode.ENFORCE
        violations += AuthViolation(
            sender.instanceId, if (code == ErrorCode.UNAUTHENTICATED) Status.Code.UNAUTHENTICATED else Status.Code.PERMISSION_DENIED,
            message, enforced, System.nanoTime(),
        )
        if (!enforced) return true
        sender.fail(req.id, code, message)
        return false
    }

    private fun forward(sender: Session, req: Request) {
        lastRequest = req
        requestsBySender.merge(sender.instanceId, 1, Int::plus)
        if (req.id.isEmpty() || req.type.isEmpty()) {
            sender.fail(req.id, ErrorCode.INVALID_REQUEST, "request id and type must not be empty")
            return
        }
        if (!maySend(sender, req)) return
        if (req.mode != RequestMode.UNARY) {
            // A v1 session cannot know UNSUPPORTED: it gets INVALID_REQUEST, like from the real hub.
            val refusal = ErrorCode.INVALID_REQUEST
            when {
                req.mode == RequestMode.SCATTER_GATHER && !sender.scatterGather ->
                    sender.fail(req.id, refusal, "scatter-gather needs the $SCATTER_GATHER capability")
                req.mode == RequestMode.SCATTER_GATHER -> scatter(sender, req)
                req.mode == RequestMode.STREAMING && !sender.streaming ->
                    sender.fail(req.id, refusal, "request mode STREAMING needs the \"$STREAMING\" capability, not declared by this session")
                req.mode == RequestMode.STREAMING -> startStream(sender, req)
                req.mode == RequestMode.SUBSCRIPTION && !sender.subscription ->
                    sender.fail(req.id, refusal, "request mode SUBSCRIPTION needs the \"$SUBSCRIPTION\" capability, not declared by this session")
                req.mode == RequestMode.SUBSCRIPTION -> startSubscription(sender, req)
                else -> sender.fail(req.id, refusal, "request mode ${req.mode} is not supported")
            }
            return
        }
        synchronized(lock) {
            if (req.id in pending || req.id in gathers || req.id in streams || req.id in subscriptions) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "request ${req.id} is already in flight")
                return
            }
            val candidates = sessions.filter { it.registered && req.type in it.types }
            if (candidates.isEmpty()) {
                sender.fail(req.id, ErrorCode.NO_HANDLER, "no handler registered for ${req.type}")
                return
            }
            val index = rotation.merge(req.type, 1, Int::plus)!! - 1
            val handler = candidates[index % candidates.size]
            val timeout = effectiveTimeout(req)
            val timer = scope.launch {
                delay(timeout.toMillis())
                if (!emitTimeouts) return@launch
                val expired = synchronized(lock) {
                    handler.queue.removeIf { it.request.id == req.id } // a queued request keeps its own deadline
                    pending.remove(req.id)
                }
                expired?.sender?.fail(req.id, ErrorCode.TIMEOUT, "no reply for ${req.type} within ${timeout.toMillis()} ms")
            }
            val permits = handler.permits
            if (permits != null && permits <= 0) {
                if (handler.queue.size >= queueCapacity) {
                    timer.cancel()
                    if (sender.flowControl) sender.fail(req.id, ErrorCode.RESOURCE_EXHAUSTED, "queue of ${handler.instanceId} is full")
                    else sender.fail(req.id, ErrorCode.HANDLER_ERROR, "handler busy")
                    return
                }
                pending[req.id] = Pending(sender, handler, timer)
                lastHeaders = req.headersMap.toMap()
                handler.queue += Queued(req, seq++)
                handler.queue.sortWith(compareByDescending<Queued> { it.request.priority }.thenBy { it.seq })
                return
            }
            pending[req.id] = Pending(sender, handler, timer)
            lastHeaders = req.headersMap.toMap()
            deliver(handler, req)
        }
    }

    private fun effectiveTimeout(req: Request): Duration {
        val override = typeTimeouts[req.type]
        return if (req.timeoutMs > 0) minOf(Duration.ofMillis(req.timeoutMs.toLong()), override ?: maxTimeout) else override ?: replyTimeout
    }

    private fun scatter(sender: Session, req: Request) {
        synchronized(lock) {
            if (req.id in pending || req.id in gathers || req.id in streams || req.id in subscriptions) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "request ${req.id} is already in flight")
                return
            }
            val owners = sessions.filter { it.registered && req.type in it.types }
            if (req.kind == RequestKind.COMMAND || owners.any { req.type in it.commandTypes }) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "scatter-gather is for queries only")
                return
            }
            if (owners.isEmpty()) {
                sender.fail(req.id, ErrorCode.NO_HANDLER, "no handler registered for ${req.type}")
                return
            }
            lastHeaders = req.headersMap.toMap()
            val gather = Gather(req.id, sender, req.maxResults)
            gathers[req.id] = gather
            val timeout = effectiveTimeout(req)
            gather.timer = scope.launch {
                delay(timeout.toMillis())
                if (!emitTimeouts) return@launch
                synchronized(lock) { closeGather(gather, timedOut = gather.remaining.size, reason = "deadline") }
            }
            val targets = owners.sortedWith(compareBy({ it.serviceName }, { it.instanceId }))
            targets.forEachIndexed { n, target -> gather.remaining += "${req.id}#$n" }
            targets.forEachIndexed { n, target ->
                val targetRequest = req.toBuilder().setId("${req.id}#$n").setMode(RequestMode.UNARY).setMaxResults(0).build()
                if (gathers[req.id] !== gather) return // every target so far failed and ended the gather
                pending[targetRequest.id] = Pending(sender, target, timer = null, gather = gather)
                val permits = target.permits
                when {
                    permits == null || permits > 0 -> deliver(target, targetRequest)
                    target.queue.size >= queueCapacity -> {
                        pending.remove(targetRequest.id)
                        val code = if (sender.flowControl) ErrorCode.RESOURCE_EXHAUSTED else ErrorCode.HANDLER_ERROR
                        val text = if (sender.flowControl) "queue of ${target.instanceId} is full" else "handler busy"
                        answered(gather, targetRequest.id, target, null, Error.newBuilder().setCode(code).setMessage(text).build())
                    }
                    else -> {
                        target.queue += Queued(targetRequest, seq++)
                        target.queue.sortWith(compareByDescending<Queued> { it.request.priority }.thenBy { it.seq })
                    }
                }
            }
        }
    }

    /**
     * Target [targetId] of [gather] answered (or failed on the hub's side): one `ReplyPart` to the sender, and the
     * `ReplyEnd` when it was the last one needed. Caller holds [lock].
     */
    private fun answered(gather: Gather, targetId: String, target: Session, reply: Reply?, error: Error?) {
        if (gathers[gather.id] !== gather || !gather.remaining.remove(targetId)) return // the gather already ended
        val part = ReplyPart.newBuilder()
            .setRequestId(gather.id)
            .setResponderService(target.serviceName)
            .setResponderInstance(target.instanceId)
            .setSeq(gather.seq++)
        val failure = error ?: reply?.takeIf { it.hasError() }?.error
        if (failure != null) {
            part.setError(failure.toBuilder().setRequestId(gather.id))
            gather.failed++
        } else {
            part.setPayload(reply!!.payload)
            gather.ok++
        }
        gather.parts++
        gather.sender.out.trySend(ServerMessage.newBuilder().setPart(part).build())
        if (gather.remaining.isEmpty() || (gather.maxResults > 0 && gather.ok >= gather.maxResults)) closeGather(gather, timedOut = 0, reason = "max results")
    }

    /**
     * Ends [gather]: its unanswered targets leave their queues and the map (a forwarded one whose session negotiated
     * `streaming` or `subscription` gets `Cancel{<id>#<n>, reason}`, P4b/P4c), then its `ReplyEnd` (unless the sender is
     * gone). Caller holds [lock].
     */
    private fun closeGather(gather: Gather, timedOut: Int, reason: String, tellSender: Boolean = true) {
        if (gathers.remove(gather.id) !== gather) return
        gather.timer.cancel()
        for (targetId in gather.remaining) {
            val entry = pending.remove(targetId) ?: continue
            val queued = entry.handler.queue.removeIf { it.request.id == targetId }
            if (!queued && entry.handler.multiPart) entry.handler.cancel(targetId, reason)
        }
        gather.remaining.clear()
        if (!tellSender) return
        val end = ReplyEnd.newBuilder().setRequestId(gather.id).setParts(gather.parts).setFailed(gather.failed).setTimedOut(timedOut)
        gather.sender.out.trySend(ServerMessage.newBuilder().setEnd(end).build())
    }

    // --- streaming queries (P4b) ---

    /** Starts the stream [req] from [sender]: the gates in the hub's order, then the target through its permits and queue. */
    private fun startStream(sender: Session, req: Request) {
        synchronized(lock) {
            if (req.kind == RequestKind.COMMAND) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "invalid request: ${req.type} is a command; streaming is for queries")
                return
            }
            val owners = sessions.filter { it.registered && req.type in it.types }
            if (owners.isEmpty()) {
                sender.fail(req.id, ErrorCode.NO_HANDLER, "no handler registered for ${req.type}")
                return
            }
            if (owners.any { req.type in it.commandTypes }) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "invalid request: ${req.type} is a command; streaming is for queries")
                return
            }
            val capable = owners.filter { it.streaming }
            if (capable.isEmpty()) {
                val services = owners.map { it.serviceName }.distinct().sorted().joinToString(", ")
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "no live instance of $services serving ${req.type} negotiated the \"$STREAMING\" capability")
                return
            }
            val targetId = "${req.id}#1"
            if (req.id in pending || req.id in gathers || req.id in streams || req.id in subscriptions || targetId in pending || targetId in streams) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "request ${req.id} is already in flight")
                return
            }
            val index = rotation.merge(req.type, 1, Int::plus)!! - 1
            val handler = capable[index % capable.size]
            // initial_credit is a uint32 (0xFFFFFFFF is a window of 4 billion parts, not -1); 0 reads as 64.
            val credit = Integer.toUnsignedLong(req.initialCredit).takeIf { it > 0 } ?: 64L
            val stream = Stream(req.id, sender, handler, credit, req.maxResults, effectiveTimeout(req))
            streams[req.id] = stream
            lastHeaders = req.headersMap.toMap()
            val max = maxStreamDuration
            stream.duration = scope.launch {
                delay(max.toMillis())
                val text = "max stream duration (${max.toMillis()} ms) exceeded"
                synchronized(lock) { closeStream(stream, error(stream.id, ErrorCode.CANCELLED, text), cancel = text) }
            }
            armIdle(stream)
            val target = req.toBuilder().setId(targetId).setInitialCredit(credit.toInt()).setMaxResults(0).build()
            pending[targetId] = Pending(sender, handler, timer = null, stream = stream)
            val permits = handler.permits
            when {
                permits == null || permits > 0 -> deliver(handler, target)
                handler.queue.size >= queueCapacity -> {
                    val code = if (sender.flowControl) ErrorCode.RESOURCE_EXHAUSTED else ErrorCode.HANDLER_ERROR
                    val text = if (sender.flowControl) "queue of ${handler.instanceId} is full" else "handler busy"
                    closeStream(stream, error(stream.id, code, text), cancel = null)
                }
                else -> {
                    handler.queue += Queued(target, seq++)
                    handler.queue.sortWith(compareByDescending<Queued> { it.request.priority }.thenBy { it.seq })
                }
            }
        }
    }

    /** Stops [stream]'s idle timer while its credit is 0 (a `Flow` restarts it). Caller holds [lock]. */
    private fun stopIdle(stream: Stream) {
        stream.idle?.cancel()
        stream.idle = null
    }

    /** (Re)starts [stream]'s idle timer: no part within its timeout ends it with `TIMEOUT`. Caller holds [lock]. */
    private fun armIdle(stream: Stream) {
        stream.idle?.cancel()
        stream.idle = scope.launch {
            delay(stream.timeout.toMillis())
            if (!emitTimeouts) return@launch
            synchronized(lock) {
                closeStream(stream, error(stream.id, ErrorCode.TIMEOUT, "no part for ${stream.id} within ${stream.timeout.toMillis()} ms"), cancel = "idle timeout")
            }
        }
    }

    /**
     * Ends [stream]: its timers stop, its target leaves the map and the handler's queue, the sender gets
     * `ReplyEnd{error?, parts}` (unless [tellSender] is false: it cancelled or left) and a forwarded target gets
     * `Cancel{<id>#1, cancel}` when [cancel] is set. Caller holds [lock].
     */
    private fun closeStream(stream: Stream, error: Error?, cancel: String?, tellSender: Boolean = true) {
        if (streams.remove(stream.id) !== stream) return
        stream.idle?.cancel()
        stream.duration.cancel()
        pending.remove(stream.targetId)
        stream.handler.queue.removeIf { it.request.id == stream.targetId }
        if (tellSender) {
            val end = ReplyEnd.newBuilder().setRequestId(stream.id).setParts(stream.parts)
            if (error != null) end.setError(error)
            stream.sender.out.trySend(ServerMessage.newBuilder().setEnd(end).build())
        }
        if (cancel != null && stream.forwarded) stream.handler.cancel(stream.targetId, cancel)
    }

    /** The open stream whose target [targetId] is served by [handler], or null (unknown, ended, or not its target). */
    private fun streamOfTarget(targetId: String, handler: Session): Stream? =
        pending[targetId]?.stream?.takeIf { it.handler === handler && streams[it.id] === it }

    private fun relayPart(handler: Session, part: ReplyPart) {
        if (!handler.multiPart) return
        synchronized(lock) {
            val stream = streamOfTarget(part.requestId, handler) ?: run {
                subscriptionOfTarget(part.requestId, handler)?.let { onSubscriptionPart(it, part.requestId, handler, part) }
                return
            }
            when {
                part.hasError() -> closeStream(stream, part.error.toBuilder().setRequestId(stream.id).build(), cancel = null)
                stream.credit <= 0 -> closeStream(stream, error(stream.id, ErrorCode.INVALID_REQUEST, "part without credit"), cancel = "part without credit")
                else -> {
                    stream.credit--
                    stream.parts++
                    val relayed = ReplyPart.newBuilder().setRequestId(stream.id).setPayload(part.payload)
                        .setResponderService(handler.serviceName).setResponderInstance(handler.instanceId).setSeq(part.seq)
                    if (part.seq !in dropParts) stream.sender.out.trySend(ServerMessage.newBuilder().setPart(relayed).build())
                    // Nothing can flow until the sender grants more: a slow consumer is no idle handler (the clock stops).
                    if (stream.credit > 0) armIdle(stream) else stopIdle(stream)
                    if (stream.maxResults > 0 && stream.parts >= stream.maxResults) closeStream(stream, null, cancel = "max results")
                }
            }
        }
    }

    private fun relayEnd(handler: Session, end: ReplyEnd) {
        if (!handler.multiPart) return
        synchronized(lock) {
            val stream = streamOfTarget(end.requestId, handler) ?: run {
                subscriptionOfTarget(end.requestId, handler)?.let { onSubscriptionEnd(it, end.requestId, handler, end) }
                return
            }
            closeStream(stream, if (end.hasError()) end.error.toBuilder().setRequestId(stream.id).build() else null, cancel = null)
        }
    }

    private fun relayFlow(sender: Session, flow: Flow) {
        flowsBySender.computeIfAbsent(sender.instanceId) { CopyOnWriteArrayList() }.add(flow.credits)
        val credits = Integer.toUnsignedLong(flow.credits) // uint32, as initial_credit
        if (!sender.multiPart || credits == 0L) return
        synchronized(lock) {
            subscriptions[flow.requestId]?.takeIf { it.sender === sender }?.let { sub ->
                // Nothing is relayed to the targets: the credit only drains what the hub holds.
                sub.credit += credits
                if (sub.initialDone && !drainSubscription(sub)) completeIfDone(sub)
                return
            }
            val stream = streams[flow.requestId]?.takeIf { it.sender === sender } ?: return
            if (stream.credit == 0L) armIdle(stream) // the idle clock was paused while nothing could flow: it starts over
            stream.credit += credits
            // Relayed at once, also while the target is still queued (the handler ignores a Flow for a request it has not seen).
            stream.handler.out.trySend(ServerMessage.newBuilder().setFlow(Flow.newBuilder().setRequestId(stream.targetId).setCredits(flow.credits)).build())
        }
    }

    private fun onCancel(sender: Session, cancel: Cancel) {
        cancelsBySender.computeIfAbsent(sender.instanceId) { CopyOnWriteArrayList() }.add(cancel)
        if (!sender.multiPart) return
        synchronized(lock) {
            subscriptions[cancel.requestId]?.takeIf { it.sender === sender }?.let { sub ->
                closeSubscription(sub, null, cancel = cancel.reason, tellSender = false)
                return
            }
            val stream = streams[cancel.requestId]?.takeIf { it.sender === sender } ?: return
            closeStream(stream, null, cancel = cancel.reason, tellSender = false)
        }
    }

    /** Forwards [req] to [handler], spending a permit when it is permit-managed. Caller holds [lock]. */
    private fun deliver(handler: Session, req: Request) {
        handler.permits = handler.permits?.minus(1)
        forwardedOrder += req.id
        handler.out.trySend(ServerMessage.newBuilder().setRequest(req).build())
        pending[req.id]?.stream?.let { stream -> // a stream's idle timeout starts over at the forward
            stream.forwarded = true
            armIdle(stream)
        }
        pending[req.id]?.subscription?.let { it.initialForwarded = true }
    }

    private fun grant(session: Session, count: Int, grant: Int) {
        permitMessages.merge(session.instanceId, 1, Int::plus)
        permitsGranted.merge(session.instanceId, count, Int::plus)
        synchronized(lock) {
            val permits = session.permits ?: return // not permit-managed: ignored, like the real hub
            if (permitGrants) session.cap = maxOf(session.cap, grant)
            session.permits = if (permitCap) minOf(permits + count, session.cap) else permits + count
            while ((session.permits ?: 0) > 0 && session.queue.isNotEmpty()) {
                val next = session.queue.removeAt(0)
                if (next.request.id in pending) deliver(session, next.request) // skip entries that already timed out
            }
        }
    }

    private fun relay(handler: Session, msg: ClientMessage) {
        val reply = msg.reply
        val entry = synchronized(lock) {
            // A unary Reply to a streaming target (not a streaming library): one part then the end, Reply{error} as the end's error.
            streamOfTarget(reply.requestId, handler)?.let { stream ->
                if (reply.hasError()) {
                    closeStream(stream, reply.error.toBuilder().setRequestId(stream.id).build(), cancel = null)
                } else {
                    stream.parts++
                    val part = ReplyPart.newBuilder().setRequestId(stream.id).setPayload(reply.payload)
                        .setResponderService(handler.serviceName).setResponderInstance(handler.instanceId).setSeq(0)
                    stream.sender.out.trySend(ServerMessage.newBuilder().setPart(part).build())
                    closeStream(stream, null, cancel = null)
                }
                return
            }
            // A unary Reply to an update target (not a subscription library) is ignored.
            if (subscriptionOfTarget(reply.requestId, handler) != null) return
            val p = pending[reply.requestId]
            val taken = if (p != null && p.handler === handler) pending.remove(reply.requestId) else null
            taken?.gather?.let { gather -> answered(gather, reply.requestId, handler, reply, null) }
            taken?.subscription?.let { sub ->
                taken.timer?.cancel()
                onSubscriptionInitial(sub, handler, reply)
            }
            taken
        } ?: return // late (after TIMEOUT or the gather's end) or unknown reply: dropped
        if (entry.gather != null || entry.subscription != null) return
        entry.timer?.cancel()
        entry.sender.out.trySend(ServerMessage.newBuilder().setReply(reply).build())
    }

    private fun cleanup(session: Session) {
        val failed = synchronized(lock) {
            sessions.remove(session)
            session.queue.clear() // its queued requests are pending with this handler: failed below
            // Gathers of this sender end silently (nobody is left to tell); forwarded streaming targets are cancelled.
            gathers.values.filter { it.sender === session }.toList().forEach { gather ->
                closeGather(gather, timedOut = gather.remaining.size, reason = "sender disconnected", tellSender = false)
            }
            // Streams it served end with HANDLER_DISCONNECTED; streams it was collecting cancel their handler.
            streams.values.filter { it.handler === session }.toList().forEach { stream ->
                closeStream(stream, error(stream.id, ErrorCode.HANDLER_DISCONNECTED, "handler ${session.instanceId} disconnected"), cancel = null)
            }
            streams.values.filter { it.sender === session }.toList().forEach { stream ->
                closeStream(stream, null, cancel = "sender disconnected", tellSender = false)
            }
            // Subscriptions it was collecting cancel every target; one whose initial request it was handling fails;
            // as a target it leaves silently unless it was the last one.
            subscriptions.values.filter { it.sender === session }.toList().forEach { sub ->
                closeSubscription(sub, null, cancel = "sender disconnected", tellSender = false)
            }
            pending.values.filter { it.subscription != null && it.handler === session }.toList().forEach { p ->
                closeSubscription(p.subscription!!, error(p.subscription.id, ErrorCode.HANDLER_DISCONNECTED, "handler ${session.instanceId} disconnected"), cancel = "initial result failed")
            }
            subscriptions.values.toList().forEach { sub ->
                val gone = sub.targets.filterValues { it === session }.keys
                gone.forEach { sub.targets.remove(it) }
                if (gone.isNotEmpty() && sub.targets.isEmpty()) {
                    closeSubscription(sub, error(sub.id, ErrorCode.HANDLER_DISCONNECTED, "handler ${session.instanceId} disconnected"), cancel = "target ${session.instanceId} disconnected")
                }
            }
            val mine = pending.filterValues { it.handler === session || it.sender === session }
            mine.keys.forEach { pending.remove(it) }
            // A target of another sender's gather fails as a part.
            mine.forEach { (id, p) ->
                p.gather?.let { answered(it, id, session, null, Error.newBuilder().setCode(ErrorCode.HANDLER_DISCONNECTED).setMessage("handler ${session.instanceId} disconnected").build()) }
            }
            mine.filterValues { it.gather == null }
        }
        failed.forEach { (id, p) ->
            p.timer?.cancel()
            if (p.handler === session && p.sender !== session) {
                p.sender.fail(id, ErrorCode.HANDLER_DISCONNECTED, "handler ${session.instanceId} disconnected")
            }
        }
    }

    // --- subscription queries (P4c) ---

    /** Starts the subscription [req] from [sender]: the gates in the hub's order, then every capable target and the initial request. */
    private fun startSubscription(sender: Session, req: Request) {
        synchronized(lock) {
            val forQueries = "invalid request: ${req.type} is a command; subscription is for queries"
            if (req.kind == RequestKind.COMMAND) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, forQueries)
                return
            }
            val owners = sessions.filter { it.registered && req.type in it.types }
            if (owners.isEmpty()) {
                sender.fail(req.id, ErrorCode.NO_HANDLER, "no handler registered for ${req.type}")
                return
            }
            if (owners.any { req.type in it.commandTypes }) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, forQueries)
                return
            }
            val capable = owners.filter { it.subscription }
            if (capable.isEmpty()) {
                val services = owners.map { it.serviceName }.distinct().sorted().joinToString(", ")
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "no live instance of $services serving ${req.type} negotiated the \"$SUBSCRIPTION\" capability")
                return
            }
            val initialId = "${req.id}#0"
            if (req.id in pending || req.id in gathers || req.id in streams || req.id in subscriptions || initialId in pending) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "invalid request: duplicate id")
                return
            }
            // The per-instance limit, over the targets of every open subscription (the last gate, under the lock).
            val max = maxSubscriptionsPerInstance
            capable.firstOrNull { target -> subscriptions.values.sumOf { s -> s.targets.values.count { it === target } } >= max }?.let { full ->
                sender.fail(req.id, ErrorCode.RESOURCE_EXHAUSTED, "handler instance ${full.instanceId} already serves $max subscription${if (max == 1) "" else "s"} (KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE)")
                return
            }
            lastHeaders = req.headersMap.toMap()
            // initial_credit is a uint32; 0 reads as 64.
            val credit = Integer.toUnsignedLong(req.initialCredit).takeIf { it > 0 } ?: 64L
            val sub = Subscription(req.id, sender, req.type, credit, req.maxResults)
            val index = rotation.merge(req.type, 1, Int::plus)!! - 1
            sub.initialHandler = capable[index % capable.size]
            capable.sortedWith(compareBy({ it.serviceName }, { it.instanceId })).forEachIndexed { n, target -> sub.targets["${req.id}#${n + 1}"] = target }
            subscriptions[req.id] = sub
            val maxDuration = maxSubscriptionDuration
            if (!maxDuration.isZero && !maxDuration.isNegative) {
                sub.duration = scope.launch {
                    delay(maxDuration.toMillis())
                    val text = "max subscription duration (${maxDuration.toMillis()} ms) exceeded"
                    synchronized(lock) { closeSubscription(sub, error(sub.id, ErrorCode.CANCELLED, text), cancel = text) }
                }
            }
            // The update targets: written directly, outside flow control (no permit, no queue).
            for ((targetId, target) in sub.targets) {
                val targetRequest = req.toBuilder().setId(targetId).setMode(RequestMode.SUBSCRIPTION).setInitialCredit(0).setMaxResults(0).build()
                forwardedOrder += targetId
                target.out.trySend(ServerMessage.newBuilder().setRequest(targetRequest).build())
            }
            // The initial result: an ordinary unary request through permits and queue, with the sender's timeout.
            val initial = req.toBuilder().setId(initialId).setMode(RequestMode.UNARY).setInitialCredit(0).setMaxResults(0).build()
            val timeout = effectiveTimeout(req)
            val handler = sub.initialHandler
            val timer = scope.launch {
                delay(timeout.toMillis())
                if (!emitTimeouts) return@launch
                synchronized(lock) {
                    if (pending.remove(initialId) == null) return@synchronized
                    handler.queue.removeIf { it.request.id == initialId }
                    closeSubscription(sub, error(sub.id, ErrorCode.TIMEOUT, "no reply for ${req.type} within ${timeout.toMillis()} ms"), cancel = "initial result failed")
                }
            }
            pending[initialId] = Pending(sender, handler, timer, subscription = sub)
            val permits = handler.permits
            when {
                permits == null || permits > 0 -> deliver(handler, initial)
                handler.queue.size >= queueCapacity -> {
                    val code = if (sender.flowControl) ErrorCode.RESOURCE_EXHAUSTED else ErrorCode.HANDLER_ERROR
                    val text = if (sender.flowControl) "queue of ${handler.instanceId} is full" else "handler busy"
                    pending.remove(initialId)
                    timer.cancel()
                    closeSubscription(sub, error(sub.id, code, text), cancel = "initial result failed")
                }
                else -> {
                    handler.queue += Queued(initial, seq++)
                    handler.queue.sortWith(compareByDescending<Queued> { it.request.priority }.thenBy { it.seq })
                }
            }
        }
    }

    /** The open subscription whose update target [targetId] is served by [handler], or null (unknown, ended, or not its target). */
    private fun subscriptionOfTarget(targetId: String, handler: Session): Subscription? {
        val parent = targetId.substringBeforeLast('#', missingDelimiterValue = "")
        return subscriptions[parent]?.takeIf { it.targets[targetId] === handler }
    }

    /** The initial request of [sub] was answered by [handler] (its pending entry already removed). Caller holds [lock]. */
    private fun onSubscriptionInitial(sub: Subscription, handler: Session, reply: Reply) {
        if (subscriptions[sub.id] !== sub) return
        if (reply.hasError()) {
            closeSubscription(sub, reply.error.toBuilder().setRequestId(sub.id).build(), cancel = "initial result failed")
            return
        }
        sub.initialDone = true
        val part = ReplyPart.newBuilder().setRequestId(sub.id).setPayload(reply.payload)
            .setResponderService(handler.serviceName).setResponderInstance(handler.instanceId)
        relaySubscriptionPart(sub, part, update = false)
        if (!drainSubscription(sub)) completeIfDone(sub)
    }

    /** An update (or error part) from target [targetId] of [sub]. Caller holds [lock]. */
    private fun onSubscriptionPart(sub: Subscription, targetId: String, handler: Session, part: ReplyPart) {
        if (part.hasError()) {
            sub.targets.remove(targetId) // it failed by itself: no cancel for it
            closeSubscription(sub, part.error.toBuilder().setRequestId(sub.id).build(), cancel = "target ${handler.instanceId} failed")
            return
        }
        val relayed = ReplyPart.newBuilder().setRequestId(sub.id).setPayload(part.payload)
            .setResponderService(handler.serviceName).setResponderInstance(handler.instanceId)
        when {
            sub.initialDone && sub.credit > 0 -> {
                relaySubscriptionPart(sub, relayed, update = true)
                if (sub.maxResults > 0 && sub.updates >= sub.maxResults) closeSubscription(sub, null, cancel = "max results")
            }
            sub.buffer.size >= subscriptionBuffer -> closeSubscription(
                sub, error(sub.id, ErrorCode.RESOURCE_EXHAUSTED, "subscriber buffer ($subscriptionBuffer) exceeded"), cancel = "subscriber buffer exceeded",
            )
            else -> sub.buffer += relayed
        }
    }

    /** Target [targetId] of [sub] ended by itself: removed; an error ends the whole subscription. Caller holds [lock]. */
    private fun onSubscriptionEnd(sub: Subscription, targetId: String, handler: Session, end: ReplyEnd) {
        sub.targets.remove(targetId)
        if (end.hasError()) closeSubscription(sub, end.error.toBuilder().setRequestId(sub.id).build(), cancel = "target ${handler.instanceId} failed")
        else completeIfDone(sub)
    }

    /** Relays [part] to [sub]'s sender with the next `seq`, taking one credit. Caller holds [lock]. */
    private fun relaySubscriptionPart(sub: Subscription, part: ReplyPart.Builder, update: Boolean) {
        sub.credit--
        sub.parts++
        if (update) sub.updates++
        val seq = sub.parts.toLong()
        if (seq !in dropParts) sub.sender.out.trySend(ServerMessage.newBuilder().setPart(part.setSeq(seq)).build())
    }

    /** Relays buffered updates while the credit allows; true when `max_results` ended the subscription. Caller holds [lock]. */
    private fun drainSubscription(sub: Subscription): Boolean {
        while (sub.credit > 0 && sub.buffer.isNotEmpty()) {
            relaySubscriptionPart(sub, sub.buffer.removeFirst(), update = true)
            if (sub.maxResults > 0 && sub.updates >= sub.maxResults) {
                closeSubscription(sub, null, cancel = "max results") // what is still buffered is dropped
                return true
            }
        }
        return false
    }

    /** Ends [sub] cleanly once every target ended, the initial part is out and the buffer is drained. Caller holds [lock]. */
    private fun completeIfDone(sub: Subscription) {
        if (sub.targets.isEmpty() && sub.initialDone && sub.buffer.isEmpty()) closeSubscription(sub, null, cancel = null)
    }

    /**
     * Ends [sub]: its timers stop, a still pending initial request leaves the map and its queue (cancelled when
     * forwarded and [cancel] is set), the sender gets `ReplyEnd{error?, parts}` (unless [tellSender] is false: it
     * cancelled or left) and every remaining target gets `Cancel{<id>#n, cancel}` when [cancel] is set. Caller holds [lock].
     */
    private fun closeSubscription(sub: Subscription, error: Error?, cancel: String?, tellSender: Boolean = true) {
        if (subscriptions.remove(sub.id) !== sub) return
        sub.duration?.cancel()
        pending.remove(sub.initialId)?.let { initial ->
            initial.timer?.cancel()
            sub.initialHandler.queue.removeIf { it.request.id == sub.initialId }
            if (cancel != null && sub.initialForwarded) sub.initialHandler.cancel(sub.initialId, cancel)
        }
        if (tellSender) {
            val end = ReplyEnd.newBuilder().setRequestId(sub.id).setParts(sub.parts)
            if (error != null) end.setError(error)
            sub.sender.out.trySend(ServerMessage.newBuilder().setEnd(end).build())
        }
        if (cancel != null) sub.targets.forEach { (targetId, target) -> target.cancel(targetId, cancel) }
        sub.targets.clear()
        sub.buffer.clear()
    }

    private companion object {
        const val FLOW_CONTROL = "flow-control"
        const val INSTANCE_STATUS = "instance-status"
        const val SCATTER_GATHER = "scatter-gather"
        const val STREAMING = "streaming"
        const val SUBSCRIPTION = "subscription"
        const val CREDENTIALS_REFRESH = "credentials-refresh"
        const val PROCESSORS = "processors"
        const val PROCESSOR_CONTROL = "processor-control"
    }

    private fun error(requestId: String, code: ErrorCode, message: String): Error =
        Error.newBuilder().setRequestId(requestId).setCode(code).setMessage(message).build()

    private fun Session.cancel(requestId: String, reason: String) {
        out.trySend(ServerMessage.newBuilder().setCancel(Cancel.newBuilder().setRequestId(requestId).setReason(reason)).build())
    }

    private fun Session.fail(requestId: String, code: ErrorCode, message: String) {
        out.trySend(
            ServerMessage.newBuilder()
                .setError(Error.newBuilder().setRequestId(requestId).setCode(code).setMessage(message)).build(),
        )
    }
}

/** How [InProcessHub.auth] treats a violation, like `KRAKEN_HUB_AUTH_MODE`: [OFF] checks nothing, [PERMISSIVE] allows and records, [ENFORCE] refuses. */
enum class AuthMode { OFF, PERMISSIVE, ENFORCE }

/**
 * One ACL entry: the [service] a token may register as, the types it may [handle] (commands, queries, shared queries) and
 * the types it may [send] in any request mode. Patterns are an exact type name, `prefix.*` (every type starting with
 * `prefix.`) or `*`.
 */
data class AclEntry(val service: String, val handle: List<String> = emptyList(), val send: List<String> = emptyList())

/**
 * The access control of an [InProcessHub]: [mode], the [tokens] (plain token -> entry; the real hub keys by SHA-256) and the
 * [services] (`[[service]]` tables, P5b: the rights of a service that authenticates with a `jwt` whose `azp` is its name).
 */
class HubAuth(val mode: AuthMode, val tokens: Map<String, AclEntry> = emptyMap(), val services: List<AclEntry> = emptyList()) {
    /** True when the hub has `[[service]]` tables, the double's stand-in for "a JWT issuer is configured" (ADR 0019). */
    val jwtEnabled: Boolean get() = services.isNotEmpty()

    /** The `[[service]]` entry named [name] (the `azp` of a JWT), or null. */
    internal fun service(name: String): AclEntry? = services.firstOrNull { it.service == name }

    companion object {
        val OFF = HubAuth(AuthMode.OFF)

        /** [services] are the `[[service]]` tables: the rights of a service that registers with a `jwt`. */
        fun enforce(tokens: Map<String, AclEntry>, services: List<AclEntry> = emptyList()) = HubAuth(AuthMode.ENFORCE, tokens, services)

        fun permissive(tokens: Map<String, AclEntry>, services: List<AclEntry> = emptyList()) = HubAuth(AuthMode.PERMISSIVE, tokens, services)

        internal fun matches(patterns: List<String>, type: String): Boolean = patterns.any {
            // `prefix.*` needs something after the dot, as the real hub's strict parsing: it does not match `prefix.` itself.
            it == "*" || it == type || (it.endsWith(".*") && type.length > it.length - 1 && type.startsWith(it.removeSuffix("*")))
        }
    }
}

/** An access-control violation seen by an [InProcessHub]: [code] is the gRPC status or error code, [enforced] false when only recorded. */
data class AuthViolation(val instanceId: String, val code: io.grpc.Status.Code, val message: String, val enforced: Boolean, val atNanos: Long)
