package io.damoreira.kraken.messaging.fixtures

import com.nimbusds.jose.JWSAlgorithm
import com.nimbusds.jose.JWSHeader
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jwt.JWTClaimsSet
import com.nimbusds.jwt.PlainJWT
import com.nimbusds.jwt.SignedJWT
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder
import java.security.KeyPairGenerator
import java.security.interfaces.RSAPrivateKey
import java.security.interfaces.RSAPublicKey
import java.util.Date
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes

/** RS256 tokens shaped like Keycloak access tokens (`realm_access.roles`), signed with a key pair made once per JVM. */
object TestJwt {
    private val keyPair = KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair()
    private val publicKey = keyPair.public as RSAPublicKey
    private val privateKey = keyPair.private as RSAPrivateKey

    fun token(
        subject: String,
        roles: List<String>,
        expiresIn: Duration = 5.minutes,
        issuer: String = "https://issuer.test/realms/kraken",
    ): String {
        val now = System.currentTimeMillis()
        val expiresAt = now + expiresIn.inWholeMilliseconds
        val claims = JWTClaimsSet.Builder()
            .subject(subject)
            .issuer(issuer)
            .issueTime(Date(minOf(now, expiresAt) - 60_000)) // issued before it expires, also for expired tokens
            .expirationTime(Date(expiresAt))
            .claim("realm_access", mapOf("roles" to roles))
            .build()
        return SignedJWT(JWSHeader(JWSAlgorithm.RS256), claims).apply { sign(RSASSASigner(privateKey)) }.serialize()
    }

    /** An unsigned (`alg: none`) token with the same claim shape; readable without a key, rejected by any decoder. */
    fun unsignedToken(subject: String?, roles: List<String>): String {
        val claims = JWTClaimsSet.Builder()
            .subject(subject)
            .issuer("https://issuer.test/realms/kraken")
            .claim("realm_access", mapOf("roles" to roles))
            .build()
        return PlainJWT(claims).serialize()
    }

    private val foreignKey = KeyPairGenerator.getInstance("RSA").apply { initialize(2048) }.generateKeyPair()

    /** An RS256 token signed by a key `decoder()` does not know: readable, but fails verification. */
    fun foreignToken(subject: String?, roles: List<String>): String {
        val claims = JWTClaimsSet.Builder()
            .subject(subject)
            .issuer("https://other.test/realms/kraken")
            .issueTime(Date())
            .expirationTime(Date(System.currentTimeMillis() + 300_000))
            .claim("realm_access", mapOf("roles" to roles))
            .build()
        return SignedJWT(JWSHeader(JWSAlgorithm.RS256), claims)
            .apply { sign(RSASSASigner(foreignKey.private as RSAPrivateKey)) }
            .serialize()
    }

    fun decoder(): JwtDecoder = NimbusJwtDecoder.withPublicKey(publicKey).build()
}
