package io.damoreira.kraken.messaging.handler

/**
 * Opts a `@CommandHandler` or `@QueryHandler` into authentication. Cross-service requests are trusted by default;
 * with this annotation the receiving library evaluates [expression] (Spring Security SpEL: `isAuthenticated()`,
 * `hasRole('x')`, `hasAnyRole(...)`, `hasAuthority(...)`, `principal`, plus `#message` and `#ctx`) against the
 * forwarded identity before invoking the handler. No identity → `UNAUTHENTICATED`; identity present but the
 * expression is false → `PERMISSION_DENIED`. Not supported on `@EventHandler` (events carry no identity).
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class RequiresAuthentication(val expression: String = "isAuthenticated()")
