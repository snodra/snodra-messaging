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
            val reg = Register.newBuilder().setServiceName(service).setInstanceId(instance).setProtocolVersion(2)
                .addAllCommandTypes(commands).addAllQueryTypes(queries).addAllSharedQueryTypes(shared)
                .addAllCapabilities(listOf("scatter-gather", "streaming", "subscription"))
            if (token != null) reg.credentials = Credentials.newBuilder().setToken(token).build()
            if (jwt != null) reg.credentials = Credentials.newBuilder().setJwt(jwt).build()
            out.trySend(ClientMessage.newBuilder().setRegister(reg).build())
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
}
