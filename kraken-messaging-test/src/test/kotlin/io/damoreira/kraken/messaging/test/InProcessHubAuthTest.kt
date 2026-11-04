package io.damoreira.kraken.messaging.test

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.Credentials
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.hub.v1.RequestMode
import io.damoreira.kraken.hub.v1.ServerMessage
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The `auth` knob of [InProcessHub] (P5a): the real hub's closes and request refusals, per mode. */
class InProcessHubAuthTest {
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val billing = "billing-" + UUID.randomUUID()
    private val orders = "orders-" + UUID.randomUUID()
    private val acl = mapOf(
        billing to AclEntry("billing-service", handle = listOf("billing.*", "shared.Ping"), send = listOf("orders.*", "files.GetFileMetadata")),
        orders to AclEntry("orders-service", handle = listOf("*"), send = listOf("*")),
    )

    @AfterEach fun tearDown() {
        scope.cancel()
        hub.close()
    }

    private inner class Raw {
        val out = Channel<ClientMessage>(Channel.UNLIMITED)
        val inbox = Channel<ServerMessage>(Channel.UNLIMITED)
        val ended = CompletableDeferred<Throwable?>()

        init {
            scope.launch {
                try {
                    HubGrpcKt.HubCoroutineStub(hub.channel()).connect(out.consumeAsFlow()).collect { inbox.send(it) }
                    ended.complete(null)
                } catch (t: Throwable) {
                    ended.complete(t)
                }
            }
        }

        fun register(
            service: String,
            instance: String,
            token: String? = null,
            jwt: String? = null,
            commands: List<String> = emptyList(),
            queries: List<String> = emptyList(),
            shared: List<String> = emptyList(),
        ) {
            val reg = Register.newBuilder().setServiceName(service).setInstanceId(instance)
                .addAllCommandTypes(commands).addAllQueryTypes(queries).addAllSharedQueryTypes(shared)
            if (token != null) reg.credentials = Credentials.newBuilder().setToken(token).build()
            if (jwt != null) reg.credentials = Credentials.newBuilder().setJwt(jwt).build()
            out.trySend(ClientMessage.newBuilder().setRegister(reg).build())
        }

        /** A `ClientMessage.credentials` (P5b): a fresh `jwt` or `token` for the session. */
        fun refresh(jwt: String? = null, token: String? = null) {
            val credentials = Credentials.newBuilder().apply { jwt?.let { setJwt(it) }; token?.let { setToken(it) } }
            out.trySend(ClientMessage.newBuilder().setCredentials(credentials).build())
        }

        fun request(type: String, mode: RequestMode, id: String = UUID.randomUUID().toString()): String {
            out.trySend(
                ClientMessage.newBuilder().setRequest(
                    Request.newBuilder().setId(id).setKind(RequestKind.QUERY).setType(type).setMode(mode)
                        .setPayload(ByteString.copyFromUtf8("{}")),
                ).build(),
            )
            return id
        }

        suspend fun next(): ServerMessage = withTimeout(5_000) { inbox.receive() }
        suspend fun status(): Status = (withTimeout(5_000) { ended.await() } as StatusException).status
    }

    private suspend fun awaitRegistered(instance: String) = withTimeout(5_000) { while (hub.acksSentTo(instance) < 1) kotlinx.coroutines.delay(5) }

    @Test fun `enforce closes a stream without credentials, with an empty token or with a jwt with UNAUTHENTICATED`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val none = Raw().also { it.register("billing-service", "i-none") }
        assertEquals(Status.Code.UNAUTHENTICATED, none.status().code)
        val empty = Raw().also { it.register("billing-service", "i-empty", token = "") }
        assertEquals(Status.Code.UNAUTHENTICATED, empty.status().code)
        val jwt = Raw().also { it.register("billing-service", "i-jwt", jwt = "a.b.c") }
        val status = jwt.status()
        assertEquals(Status.Code.UNAUTHENTICATED, status.code)
        assertTrue(status.description!!.contains("jwt"), status.toString())
        assertEquals(3, hub.authViolations().count { it.enforced })
    }

    @Test fun `enforce closes an unknown token with UNAUTHENTICATED, naming nothing of the token`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val raw = Raw().also { it.register("billing-service", "i-1", token = "not-in-the-acl") }
        val status = raw.status()
        assertEquals(Status.Code.UNAUTHENTICATED, status.code)
        assertTrue(!status.description!!.contains("not-in-the-acl"))
        assertEquals(listOf<String?>("not-in-the-acl"), hub.tokensSeenFrom("i-1"))
    }

    @Test fun `enforce closes a service name the token does not grant with PERMISSION_DENIED`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val raw = Raw().also { it.register("orders-service", "i-1", token = billing) }
        assertEquals(Status.Code.PERMISSION_DENIED, raw.status().code)
    }

    @Test fun `enforce closes a handled type outside handle with PERMISSION_DENIED naming it, for commands, queries and shared queries`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val command = Raw().also { it.register("billing-service", "i-1", token = billing, commands = listOf("billing.Charge", "orders.Place", "orders.Cancel")) }
        val status = command.status()
        assertEquals(Status.Code.PERMISSION_DENIED, status.code)
        assertTrue(status.description!!.contains("orders.Place") && status.description!!.contains("1 more"), status.toString())
        assertEquals(Status.Code.PERMISSION_DENIED, Raw().also { it.register("billing-service", "i-2", token = billing, queries = listOf("orders.Get")) }.status().code)
        assertEquals(Status.Code.PERMISSION_DENIED, Raw().also { it.register("billing-service", "i-3", token = billing, shared = listOf("orders.Get")) }.status().code)
    }

    @Test fun `a right token registers and is acknowledged, patterns match by prefix, exactly and by star`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val raw = Raw().also { it.register("billing-service", "i-1", token = billing, commands = listOf("billing.Charge", "billing.deep.Nested"), queries = listOf("shared.Ping")) }
        assertTrue(raw.next().hasRegisterAck())
        val star = Raw().also { it.register("orders-service", "i-2", token = orders, commands = listOf("anything.AtAll")) }
        assertTrue(star.next().hasRegisterAck())
        // The bare prefix is not matched by "prefix.*".
        assertEquals(Status.Code.PERMISSION_DENIED, Raw().also { it.register("billing-service", "i-3", token = billing, commands = listOf("billing")) }.status().code)
        assertTrue(hub.authViolations().none { it.instanceId == "i-1" || it.instanceId == "i-2" })
    }

    @Test fun `a later Register that adds a forbidden type closes the stream`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val raw = Raw().also { it.register("billing-service", "i-1", token = billing, commands = listOf("billing.Charge")) }
        assertTrue(raw.next().hasRegisterAck())
        raw.register("billing-service", "i-1", commands = listOf("billing.Charge", "orders.Place")) // credentials are read from the first Register only
        assertEquals(Status.Code.PERMISSION_DENIED, raw.status().code)
    }

    @Test fun `a request outside send is an immediate Error PERMISSION_DENIED in every request mode, nothing is routed`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val handler = Raw().also { it.register("orders-service", "h-1", token = orders, queries = listOf("secret.Get")) }
        assertTrue(handler.next().hasRegisterAck())
        val sender = Raw().also { it.register("billing-service", "s-1", token = billing) }
        assertTrue(sender.next().hasRegisterAck())
        for (mode in listOf(RequestMode.UNARY, RequestMode.SCATTER_GATHER, RequestMode.STREAMING, RequestMode.SUBSCRIPTION)) {
            val id = sender.request("secret.Get", mode)
            val error = sender.next().error
            assertEquals(id, error.requestId, mode.name)
            assertEquals(ErrorCode.PERMISSION_DENIED, error.code, mode.name)
            assertTrue(error.message.contains("secret.Get"), mode.name)
        }
        assertNull(withTimeoutOrNull(100) { handler.inbox.receive() }, "the handler heard nothing")
        // A type inside send is routed as usual (here: nobody handles it, so NO_HANDLER, not PERMISSION_DENIED).
        sender.request("orders.Get", RequestMode.UNARY)
        assertEquals(ErrorCode.NO_HANDLER, sender.next().error.code)
    }

    @Test fun `a request after the token left the ACL is UNAUTHENTICATED`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val sender = Raw().also { it.register("billing-service", "s-1", token = billing) }
        assertTrue(sender.next().hasRegisterAck())
        // The real hub closes the stream on a reload; this is the race where a request gets in before that happens.
        hub.closeSessionsOnAuthChange = false
        hub.auth = HubAuth.enforce(acl - billing)
        sender.request("orders.Get", RequestMode.UNARY)
        assertEquals(ErrorCode.UNAUTHENTICATED, sender.next().error.code)
    }

    @Test fun `permissive allows and records every violation`() = runBlocking {
        hub.auth = HubAuth.permissive(acl)
        val anonymous = Raw().also { it.register("whoever", "i-anon", commands = listOf("x.Cmd")) }
        assertTrue(anonymous.next().hasRegisterAck())
        val wrongService = Raw().also { it.register("orders-service", "i-wrong", token = billing, commands = listOf("o.Cmd")) }
        assertTrue(wrongService.next().hasRegisterAck())
        val sender = Raw().also { it.register("billing-service", "i-ok", token = billing) }
        assertTrue(sender.next().hasRegisterAck())
        sender.request("secret.Get", RequestMode.UNARY)
        assertEquals(ErrorCode.NO_HANDLER, sender.next().error.code) // routed, as nothing was refused

        val violations = hub.authViolations()
        assertTrue(violations.none { it.enforced })
        assertEquals(Status.Code.UNAUTHENTICATED, violations.single { it.instanceId == "i-anon" }.code)
        // wrong service name and the type outside handle, both for i-wrong; the request outside send for i-ok
        assertEquals(listOf(Status.Code.PERMISSION_DENIED, Status.Code.PERMISSION_DENIED), violations.filter { it.instanceId == "i-wrong" }.map { it.code })
        assertEquals(Status.Code.PERMISSION_DENIED, violations.single { it.instanceId == "i-ok" }.code)
        // A session without a valid token is not judged per request (ruling 14).
        anonymous.request("secret.Get", RequestMode.UNARY)
        assertEquals(ErrorCode.NO_HANDLER, anonymous.next().error.code)
        assertEquals(1, hub.authViolations().count { it.instanceId == "i-anon" })
    }

    @Test fun `off ignores the token but still records which one each Register carried`() = runBlocking {
        val raw = Raw().also { it.register("anything", "i-1", token = "t-1") }
        assertTrue(raw.next().hasRegisterAck())
        awaitRegistered("i-1")
        raw.register("anything", "i-1", token = "t-1", commands = listOf("a.B"))
        withTimeout(5_000) { while (hub.registrationsFrom("i-1") < 2) kotlinx.coroutines.delay(5) }
        assertEquals(listOf<String?>("t-1", "t-1"), hub.tokensSeenFrom("i-1"))
        assertEquals(emptyList(), hub.authViolations())
    }

    // --- fidelity with the real hub on reload and on strict pattern parsing ---

    @Test fun `a prefix star pattern does not match the bare prefix itself`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val bare = Raw().also { it.register("billing-service", "i-1", token = billing, commands = listOf("billing.")) }
        val status = bare.status()
        assertEquals(Status.Code.PERMISSION_DENIED, status.code)
        assertTrue(status.description!!.contains("billing."), status.toString())
        // ... while a type with something after the dot still matches.
        val ok = Raw().also { it.register("billing-service", "i-2", token = billing, commands = listOf("billing.X")) }
        assertTrue(ok.next().hasRegisterAck())
    }

    @Test fun `replacing auth in enforce closes live sessions the new ACL no longer allows, with the real hub's codes`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val revoked = Raw().also { it.register("billing-service", "i-revoked", token = billing, commands = listOf("billing.Charge")) }
        val narrowed = Raw().also { it.register("orders-service", "i-narrowed", token = orders, commands = listOf("orders.Place", "other.Thing")) }
        val renamed = Raw().also { it.register("orders-service", "i-renamed", token = orders, commands = listOf("orders.Cancel")) }
        val kept = Raw().also { it.register("orders-service", "i-kept", token = orders, queries = listOf("orders.Get")) }
        for (raw in listOf(revoked, narrowed, renamed, kept)) assertTrue(raw.next().hasRegisterAck())

        hub.auth = HubAuth.enforce(
            mapOf(
                // billing's token is gone; the orders token keeps its service but only handles orders.*
                orders to AclEntry("orders-service", handle = listOf("orders.*"), send = listOf("*")),
            ),
        )
        // revoked: token left the ACL -> UNAUTHENTICATED; narrowed: handles other.Thing no more -> PERMISSION_DENIED.
        assertEquals(Status.Code.UNAUTHENTICATED, revoked.status().code)
        assertEquals(Status.Code.PERMISSION_DENIED, narrowed.status().code)
        assertTrue(hub.sessionsOf("i-revoked") == 0 && hub.sessionsOf("i-narrowed") == 0)
        assertEquals(1, hub.sessionsOf("i-renamed"))
        assertEquals(1, hub.sessionsOf("i-kept"))

        // The token now grants another service: a live session registered as orders-service is closed with PERMISSION_DENIED.
        hub.auth = HubAuth.enforce(mapOf(orders to AclEntry("someone-else", handle = listOf("*"), send = listOf("*"))))
        assertEquals(Status.Code.PERMISSION_DENIED, renamed.status().code)
        assertEquals(Status.Code.PERMISSION_DENIED, kept.status().code)
        assertEquals(4, hub.authViolations().count { it.enforced })
    }

    @Test fun `replacing auth fails the in-flight requests of a closed handler and never closes sessions in permissive or off`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val handler = Raw().also { it.register("orders-service", "h-1", token = orders, queries = listOf("orders.Get")) }
        assertTrue(handler.next().hasRegisterAck())
        val sender = Raw().also { it.register("billing-service", "s-1", token = billing) }
        assertTrue(sender.next().hasRegisterAck())
        val id = sender.request("orders.Get", RequestMode.UNARY)
        assertEquals(id, handler.next().request.id)

        hub.auth = HubAuth.permissive(emptyMap()) // nothing is closed in permissive
        assertNull(withTimeoutOrNull(200) { handler.ended.await() })
        hub.auth = HubAuth.OFF
        assertNull(withTimeoutOrNull(200) { handler.ended.await() })
        assertEquals(1, hub.sessionsOf("h-1"))
        assertTrue(hub.authViolations().isEmpty())

        hub.auth = HubAuth.enforce(acl - orders) // the handler's token is gone, the sender's stays
        assertEquals(Status.Code.UNAUTHENTICATED, handler.status().code)
        val error = sender.next().error
        assertEquals(id, error.requestId)
        assertEquals(ErrorCode.HANDLER_DISCONNECTED, error.code)
    }

    // --- instance takeover (the real hub refuses a Register of another service's instance id) ---

    @Test fun `a Register of another service's instance id is PERMISSION_DENIED in enforce and permissive and leaves the owner untouched`() = runBlocking {
        for (mode in listOf(HubAuth.enforce(acl), HubAuth.permissive(acl))) {
            val instance = "victim-" + UUID.randomUUID()
            hub.auth = mode
            val victim = Raw().also { it.register("orders-service", instance, token = orders, commands = listOf("orders.Place")) }
            assertTrue(victim.next().hasRegisterAck())
            val attacker = Raw().also { it.register("billing-service", instance, token = billing, commands = listOf("billing.Charge")) }
            val status = attacker.status()
            assertEquals(Status.Code.PERMISSION_DENIED, status.code, mode.mode.name)
            assertEquals("instance id ${instance.take(8)} belongs to another service", status.description, mode.mode.name)
            assertEquals(1, hub.sessionsOf(instance), mode.mode.name)
            assertEquals(0, hub.sessionsReplacedOf(instance), mode.mode.name)
            assertEquals(setOf("orders.Place"), hub.registeredTypes(), mode.mode.name)
            assertNull(withTimeoutOrNull(100) { victim.ended.await() }, "the owner's stream stays up")
            victim.out.close()
            withTimeout(5_000) { while (hub.sessionsOf(instance) > 0) kotlinx.coroutines.delay(5) }
        }
    }

    @Test fun `with auth off an instance id is still replaced whatever its service, and a same-service reconnect takes over in enforce`() = runBlocking {
        val shared = "shared-" + UUID.randomUUID()
        val first = Raw().also { it.register("orders-service", shared) }
        assertTrue(first.next().hasRegisterAck())
        val second = Raw().also { it.register("billing-service", shared) }
        assertTrue(second.next().hasRegisterAck())
        assertEquals(1, hub.sessionsReplacedOf(shared))

        hub.auth = HubAuth.enforce(acl)
        val own = "own-" + UUID.randomUUID()
        val old = Raw().also { it.register("orders-service", own, token = orders) }
        assertTrue(old.next().hasRegisterAck())
        val reconnect = Raw().also { it.register("orders-service", own, token = orders) }
        assertTrue(reconnect.next().hasRegisterAck())
        assertEquals(1, hub.sessionsReplacedOf(own))
    }

    // --- JWT credentials (P5b, ADR 0019) ---

    /** An unsigned token shaped like a client-credentials access token: the hub double reads `azp` and `exp`, never the signature. */
    private fun jwt(azp: String, expiresInSeconds: Long = 300) = listOf(
        """{"alg":"RS256","typ":"JWT"}""",
        """{"azp":"$azp","exp":${System.currentTimeMillis() / 1000 + expiresInSeconds}}""",
    ).joinToString(".", postfix = ".sig") { java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(it.toByteArray()) }

    private val jwtServices = listOf(AclEntry("billing-service", handle = listOf("billing.*"), send = listOf("orders.*")))

    @Test fun `enforce accepts a jwt whose azp is the service name and that service has an ACL entry, and records it`() = runBlocking {
        hub.auth = HubAuth.enforce(acl, jwtServices)
        val token = jwt("billing-service")
        val raw = Raw().also { it.register("billing-service", "i-jwt", jwt = token, commands = listOf("billing.Charge")) }
        assertTrue(raw.next().hasRegisterAck())
        assertEquals(listOf(token), hub.jwtsSeenFrom("i-jwt"))
        assertEquals(listOf<String?>(null), hub.tokensSeenFrom("i-jwt"))
        assertTrue(hub.authViolations().isEmpty())
        // the rights come from the service's entry: send is orders.*
        raw.request("secret.Get", RequestMode.UNARY)
        assertEquals(ErrorCode.PERMISSION_DENIED, raw.next().error.code)
    }

    @Test fun `enforce refuses a jwt of another service with PERMISSION_DENIED, a service without an entry too, and a hub with no jwt setup with UNAUTHENTICATED`() = runBlocking {
        hub.auth = HubAuth.enforce(acl, jwtServices)
        val wrongAzp = Raw().also { it.register("billing-service", "i-1", jwt = jwt("orders-service")) }
        val status = wrongAzp.status()
        assertEquals(Status.Code.PERMISSION_DENIED, status.code)
        assertTrue("orders-service" in status.description!!, status.toString())
        val noEntry = Raw().also { it.register("files-service", "i-2", jwt = jwt("files-service")) }
        assertEquals(Status.Code.PERMISSION_DENIED, noEntry.status().code)
        val garbage = Raw().also { it.register("billing-service", "i-3", jwt = "not-a-jwt") }
        assertEquals(Status.Code.UNAUTHENTICATED, garbage.status().code)

        hub.auth = HubAuth.enforce(acl) // no [[service]] tables: jwt is not configured
        val off = Raw().also { it.register("billing-service", "i-4", jwt = jwt("billing-service")) }
        val notConfigured = off.status()
        assertEquals(Status.Code.UNAUTHENTICATED, notConfigured.code)
        assertTrue("not configured" in notConfigured.description!!, notConfigured.toString())
    }

    @Test fun `the ack offers credentials-refresh to a client that declared it, and ClientMessage credentials are recorded`() = runBlocking {
        hub.auth = HubAuth.enforce(acl, jwtServices)
        val first = jwt("billing-service")
        val raw = Raw().also { it.register("billing-service", "i-1", jwt = first) }
        assertTrue(raw.next().hasRegisterAck())
        val second = jwt("billing-service")
        raw.refresh(jwt = second)
        withTimeout(5_000) { while (hub.credentialsSeenFrom("i-1").isEmpty()) kotlinx.coroutines.delay(5) }
        assertEquals(listOf(second), hub.credentialsSeenFrom("i-1"))
        assertEquals(1, hub.sessionsOf("i-1"))
        // a client that did not declare the capability is not offered it
        val plain = Raw()
        plain.out.trySend(
            ClientMessage.newBuilder().setRegister(Register.newBuilder().setServiceName("billing-service").setInstanceId("i-2")
                .setCredentials(Credentials.newBuilder().setJwt(jwt("billing-service")))).build(),
        )
        assertTrue(plain.next().hasRegisterAck())
    }

    @Test fun `enforce closes the stream when a refresh names another service, or is not a valid credential`() = runBlocking {
        hub.auth = HubAuth.enforce(acl, jwtServices)
        val other = Raw().also { it.register("billing-service", "i-1", jwt = jwt("billing-service")) }
        assertTrue(other.next().hasRegisterAck())
        other.refresh(jwt = jwt("orders-service"))
        assertEquals(Status.Code.PERMISSION_DENIED, other.status().code)
        val bad = Raw().also { it.register("billing-service", "i-2", jwt = jwt("billing-service")) }
        assertTrue(bad.next().hasRegisterAck())
        bad.refresh(jwt = "junk")
        assertEquals(Status.Code.UNAUTHENTICATED, bad.status().code)
    }

    @Test fun `a static-token session may refresh with a token of its own service`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val raw = Raw().also { it.register("billing-service", "i-1", token = billing) }
        assertTrue(raw.next().hasRegisterAck())
        val rotated = "rotated-" + UUID.randomUUID()
        hub.auth = HubAuth.enforce(acl + (rotated to AclEntry("billing-service", handle = listOf("billing.*"), send = listOf("orders.*"))))
        raw.refresh(token = rotated)
        withTimeout(5_000) { while (hub.credentialsSeenFrom("i-1").isEmpty()) kotlinx.coroutines.delay(5) }
        assertEquals(listOf(rotated), hub.credentialsSeenFrom("i-1"))
        assertEquals(1, hub.sessionsOf("i-1"))
    }
}
