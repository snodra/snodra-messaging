package com.damoreira.snodra.examples.b

import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.http.client.JdkClientHttpRequestFactory
import org.springframework.web.client.RestClient
import java.net.http.HttpClient
import java.time.Duration

/** The printer client (not named `printer`: that name is the translator bean of the source of the same name): `examples.printer.url` (env `PRINTER_URL`), HTTP/1.1, short timeouts so a dead printer is noticed in seconds. */
@Configuration(proxyBeanMethods = false)
class PrinterConfiguration {
    @Bean
    fun printerClient(@Value("\${examples.printer.url}") url: String): Printer {
        val client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(3)).build()
        val factory = JdkClientHttpRequestFactory(client).apply { setReadTimeout(Duration.ofSeconds(5)) }
        return HttpPrinter(RestClient.builder().baseUrl(url).requestFactory(factory).build())
    }
}
