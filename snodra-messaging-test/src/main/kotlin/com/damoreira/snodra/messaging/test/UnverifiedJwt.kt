package com.damoreira.snodra.messaging.test

import tools.jackson.databind.json.JsonMapper
import java.util.Base64

/**
 * The claim [InProcessHub] reads from a `jwt` credential: `azp`, the service the token names.
 * The signature is **not** verified: the double only decides what the real hub would decide from the claims.
 */
internal data class UnverifiedJwt(val azp: String) {
    companion object {
        private val mapper = JsonMapper.builder().build()

        /** Null when [jwt] is not three dot-separated segments with a JSON object payload that has a non-empty `azp`. */
        fun parse(jwt: String): UnverifiedJwt? = try {
            val parts = jwt.split('.')
            if (parts.size != 3) null
            else {
                val payload = mapper.readTree(Base64.getUrlDecoder().decode(parts[1]))
                val azp = payload.get("azp")?.takeIf { it.isString }?.asString()?.takeIf { it.isNotEmpty() }
                azp?.let(::UnverifiedJwt)
            }
        } catch (_: Exception) {
            null
        }
    }
}
