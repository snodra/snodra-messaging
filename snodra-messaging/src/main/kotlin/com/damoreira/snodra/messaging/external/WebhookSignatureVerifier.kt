package com.damoreira.snodra.messaging.external

import java.security.MessageDigest
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec

/**
 * Verifies a webhook request against a shared secret (spec "Inbound 2"), by the rules of the Keycloak webhook of the sso-service:
 *
 * - **shared secret**: the header `X-Snodra-Webhook-Secret` holds the secret itself;
 * - **HMAC**: the header `X-Snodra-Signature` holds `sha256=<hex>`, the HMAC-SHA256 of the raw body keyed by the secret (the
 *   `sha256=` prefix is optional, the hex case does not matter).
 *
 * Either header, matching the current secret or, during a rotation, the previous one, verifies the request. Header names are
 * case-insensitive. Every comparison is constant time ([MessageDigest.isEqual]; [compare] is the seam the test uses to see it
 * is the only way a secret or signature is compared). Empty secrets are ignored: a service with none configured verifies nothing.
 *
 * Verify the **raw** body bytes, before parsing it.
 */
class WebhookSignatureVerifier(private val compare: (ByteArray, ByteArray) -> Boolean = MessageDigest::isEqual) {

    fun verify(secret: String, previousSecret: String?, headers: Map<String, String>, body: ByteArray): Boolean {
        val secrets = listOfNotNull(secret, previousSecret).filter { it.isNotEmpty() }
        val sharedSecret = header(headers, SECRET_HEADER)
        if (!sharedSecret.isNullOrEmpty() && secrets.any { compare(it.toByteArray(), sharedSecret.toByteArray()) }) return true
        val signature = header(headers, SIGNATURE_HEADER)
        if (!signature.isNullOrEmpty()) {
            val given = (if (signature.startsWith("sha256=", ignoreCase = true)) signature.substring(7) else signature).lowercase()
            return secrets.any { compare(hmacHex(it, body).toByteArray(), given.toByteArray()) }
        }
        return false
    }

    private fun header(headers: Map<String, String>, name: String): String? =
        headers.entries.firstOrNull { it.key.equals(name, ignoreCase = true) }?.value

    companion object {
        const val SECRET_HEADER = "X-Snodra-Webhook-Secret"
        const val SIGNATURE_HEADER = "X-Snodra-Signature"

        /** The HMAC-SHA256 of [body] keyed by [secret], lowercase hex: what a partner computes for `X-Snodra-Signature`. */
        fun hmacHex(secret: String, body: ByteArray): String {
            val mac = Mac.getInstance("HmacSHA256")
            mac.init(SecretKeySpec(secret.toByteArray(), "HmacSHA256"))
            return mac.doFinal(body).joinToString("") { "%02x".format(it) }
        }
    }
}
