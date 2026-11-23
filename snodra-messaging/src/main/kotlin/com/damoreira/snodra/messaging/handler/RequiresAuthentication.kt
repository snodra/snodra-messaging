package com.damoreira.snodra.messaging.handler

/**
 * Opts a `@CommandHandler` or `@QueryHandler` into authentication. Cross-service requests are trusted by default;
 * with this annotation the receiving library evaluates [expression] (Spring Security SpEL: `isAuthenticated()`,
 * `hasRole('x')`, `hasAnyRole(...)`, `hasAuthority(...)`, `principal`, plus `#message` and `#ctx`) against the
 * forwarded identity before invoking the handler. No identity → `UNAUTHENTICATED`; identity present but the
 * expression is false → `PERMISSION_DENIED`. Not supported on `@EventHandler` (events carry no identity).
 *
 * A forwarded token is an identity only once the service verified it against its JWKS (a `JwtDecoder` bean, which Spring Boot
 * creates from `spring.security.oauth2.resourceserver.jwt.jwk-set-uri` or `issuer-uri`): a service with a handler annotated
 * this way and no `JwtDecoder` fails at startup, naming the handler.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class RequiresAuthentication(val expression: String = "isAuthenticated()")
