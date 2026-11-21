package com.damoreira.snodra.messaging.hub

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.util.concurrent.atomic.AtomicLongArray
import kotlin.random.Random

/**
 * The access token of this service, one per process and shared by every hub connection: N hubs, one token.
 *
 * - [token] returns the cached token while it is fresh, else fetches; callers that arrive while a fetch runs wait for that one
 *   (single flight, and its failure is theirs too). Fresh means before [AccessToken.refreshAt] by the wall clock **and** by the
 *   monotonic clock ([nanoTime], counted from the fetch): a machine that slept stops the monotonic clock, a wall clock set back
 *   stops the other, and either alone would keep a token past its refresh point, as the Rust library does.
 * - After the first fetch the provider renews the token **in the background** when it stops being fresh and publishes it in
 *   [current], which every live stream follows (see [HubConnection]). A token a hub refused ([invalidate]) is renewed at the
 *   renewer's next look (within 5 s) and is not [usable] meanwhile. A renewal that fails keeps the current
 *   token and is retried with a back-off from [retryMin] to [retryMax] (1 s to 30 s) until it works: one WARN per distinct
 *   error, then DEBUG. The hub closes the session at `exp` plus its grace if it never does.
 * - [lastError] is the latest failure's message (null after a success), [fetches] counts the fetches by [TokenFetchResult].
 *
 * The background work runs in [scope]; cancelling it stops the renewal. Tokens never appear in [toString] or in logs.
 */
class HubTokenProvider(
    private val endpoint: TokenEndpoint,
    private val scope: CoroutineScope,
    private val clock: Clock = Clock.systemUTC(),
    private val retryMin: Duration = Duration.ofSeconds(1),
    private val retryMax: Duration = Duration.ofSeconds(30),
    private val nanoTime: () -> Long = System::nanoTime,
) {
    private val log = LoggerFactory.getLogger(HubTokenProvider::class.java)

    private val currentState = MutableStateFlow<AccessToken?>(null)
    /** The latest token fetched; null before the first one. Replaced by each renewal. */
    val current: StateFlow<AccessToken?> = currentState.asStateFlow()

    private val errorState = MutableStateFlow<String?>(null)
    /** The message of the latest failed fetch (the endpoint's `error`, or the HTTP status, clipped); null once a fetch succeeded. */
    val lastError: StateFlow<String?> = errorState.asStateFlow()

    private val counts = AtomicLongArray(TokenFetchResult.entries.size)

    /** Fetches so far with [result], for `snodra_hub_client_token_fetches_total{result}`. */
    fun fetches(result: TokenFetchResult): Long = counts.get(result.ordinal)

    private val lock = Any()
    private var inflight: Deferred<AccessToken>? = null
    private var renewer: kotlinx.coroutines.Job? = null

    /**
     * A token that is still worth using: the cached one while it is fresh, else a new one (one fetch for every caller waiting).
     *
     * @throws HubTokenException when the endpoint cannot give one
     */
    suspend fun token(): AccessToken {
        currentState.value?.takeIf { untilRefresh(it).isPositive }?.let { return it }
        return fetchShared().await()
    }

    /** The current token unless a hub refused it (see [invalidate]): what `authStatus` reports. */
    fun usable(): AccessToken? = currentState.value?.takeIf { it.value != refused }

    /** [System.nanoTime] when the current token was stored: its age by the monotonic clock. */
    @Volatile private var fetchedNanos = 0L

    /** How long until [token] stops being fresh, the earlier of the wall clock's and the monotonic clock's view; zero once it is not. */
    private fun untilRefresh(token: AccessToken): Duration {
        if (token.value == refused) return Duration.ZERO
        val wall = Duration.between(clock.instant(), token.refreshAt)
        val monotonic = token.lifetime.minus(token.refreshMargin).minusNanos(nanoTime() - fetchedNanos)
        val left = if (wall < monotonic) wall else monotonic
        return if (left.isNegative) Duration.ZERO else left
    }

    /** The value of the current token once a hub refused it with `UNAUTHENTICATED` (see [invalidate]); cleared by the next fetch. */
    @Volatile private var refused: String? = null

    /**
     * The hub refused [value] with `UNAUTHENTICATED` (a Register refusal or a later close: expired, unknown signing key, ...): the
     * cached token is not handed out again, so the next [token] call fetches a new one. A token that is no longer the current
     * one (another stream already got a newer token) changes nothing. Not for `PERMISSION_DENIED`: a new token would not help.
     */
    fun invalidate(value: String) {
        if (currentState.value?.value == value) refused = value
    }

    private fun fetchShared(): Deferred<AccessToken> = synchronized(lock) {
        inflight?.takeIf { it.isActive } ?: CompletableDeferred<AccessToken>().also { deferred ->
            inflight = deferred
            scope.launch {
                try {
                    deferred.complete(fetch())
                } catch (e: Throwable) {
                    deferred.completeExceptionally(e)
                    if (e is CancellationException) throw e
                }
            }
        }
    }

    private suspend fun fetch(): AccessToken {
        try {
            val token = endpoint.fetch()
            counts.incrementAndGet(TokenFetchResult.OK.ordinal)
            fetchedNanos = nanoTime()
            // A fetched token is usable until a hub refuses it, even when the endpoint answered the refused value again (as Rust,
            // which drops a refused token and remembers no value): otherwise the renewer would fetch again without a pause.
            refused = null
            currentState.value = token
            errorState.value = null
            log.debug("Hub access token fetched, valid until {}", token.expiresAt)
            startRenewer()
            return token
        } catch (e: HubTokenException) {
            counts.incrementAndGet(e.result.ordinal)
            errorState.value = e.message
            throw e
        }
    }

    private fun startRenewer() = synchronized(lock) {
        if (renewer == null) renewer = scope.launch { renew() }
    }

    /** The background renewal: sleeps until the current token is due (or set aside), then fetches; a failure waits and retries. */
    private suspend fun renew() {
        var backoff = retryMin
        var lastLogged: String? = null
        while (true) {
            val token = currentState.value ?: return
            val wait = untilRefresh(token)
            if (wait.isPositive) {
                // One slice at a time: `delay` counts monotonic time, which stops while the machine sleeps, so after a suspend the wall
                // clock (which also decides freshness) is ahead of it. Looking again every slice bounds the overrun to one slice, and
                // a token set aside by [invalidate] meanwhile is renewed at the next look.
                delay((if (wait > MAX_SLICE) MAX_SLICE else wait).toMillis().coerceAtLeast(1))
                continue // somebody may have renewed meanwhile, or the clock jumped: look again
            }
            try {
                fetchShared().await()
                backoff = retryMin
                lastLogged = null
            } catch (e: HubTokenException) {
                val retry = ReconnectBackoff.jittered(backoff, retryMax)
                val text = "Renewing the hub access token failed ({}); keeping the current one, retrying in {}"
                if (e.message != lastLogged) {
                    lastLogged = e.message
                    log.warn(text, e.message, retry)
                } else {
                    log.debug(text, e.message, retry)
                }
                backoff = ReconnectBackoff.next(backoff, retryMax)
                delay(retry.toMillis().coerceAtLeast(1))
            }
        }
    }

    override fun toString() = "HubTokenProvider($endpoint)"

    private companion object {
        val MAX_SLICE: Duration = Duration.ofSeconds(5)
    }
}
