package io.damoreira.kraken.conformance

import io.damoreira.kraken.messaging.test.KrakenContainers
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The Kotlin conformance service against the conformance runner (`kraken-conformance`, kraken-hub `conformance/`): every check
 * of level 1 on one real hub (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`) and the runner's fake hubs, then the two-hub part of
 * `L1-HUBS` on two. Needs Docker and the runner binary: `KRAKEN_CONFORMANCE_BIN=/path/to/kraken-conformance ./gradlew
 * :examples:conformance-service:test`; without the binary the test is skipped, saying so.
 *
 * It asserts exactly the known failures of this library ([KNOWN_FAILURES]), so a gap stays visible and a fix (or a new failure)
 * changes the test.
 */
class ConformanceLevel1Test {
    private val mapper = JsonMapper.builder().build()

    @Test
    fun kotlin_reference_passes_level_1() {
        val runner = System.getProperty("conformance.runner")?.let(::File)
        assumeTrue(
            runner != null && runner.canExecute(),
            "the conformance runner is not available: set KRAKEN_CONFORMANCE_BIN to the kraken-conformance binary " +
                "(cargo build --release -p kraken-conformance in kraken-hub/conformance)",
        )
        assumeTrue(KrakenContainers.hubImageAvailable(), "the hub image ${System.getenv("KRAKEN_HUB_IMAGE") ?: "kraken-hub:dev"} is not available locally")
        val jar = File(System.getProperty("conformance.jar"))
        val hub1 = KrakenContainers.hub(emptyMap()).also { it.start() }
        val wrong = mutableListOf<String>()
        try {
            val a = "127.0.0.1:${hub1.getMappedPort(7000)}"
            // One hub: every check (L1-HUBS on fake hubs only).
            val report = run(runner!!, jar, listOf(a), only = null)
            wrong += expect(report)
            // Two hubs: L1-HUBS with its part on two real hubs.
            val hub2 = KrakenContainers.hub(emptyMap()).also { it.start() }
            try {
                val two = run(runner, jar, listOf(a, "127.0.0.1:${hub2.getMappedPort(7000)}"), only = "L1-HUBS")
                val hubs = two.path("checks").firstOrNull { it.path("id").asString() == "L1-HUBS" }
                if (hubs == null || !hubs.path("detail").asString().contains("registered on both real hubs")) {
                    wrong += "L1-HUBS did not run its two-hub part: ${hubs?.path("detail")?.asString()}"
                }
                wrong += expect(two)
            } finally {
                hub2.stop()
            }
        } finally {
            hub1.stop()
        }
        assertTrue(wrong.isEmpty(), wrong.joinToString("\n"))
    }

    /** Runs the runner (JSON report, the text one printed too) and returns the report. */
    private fun run(runner: File, jar: File, hubs: List<String>, only: String?): JsonNode {
        val work = File(System.getProperty("conformance.work"), "run-${System.currentTimeMillis()}").apply { mkdirs() }
        val java = ProcessHandle.current().info().command().orElse("java")
        val command = mutableListOf(
            runner.path, "run",
            "--service-cmd", "'$java' -jar '${jar.path}'",
            "--service", "conformance-kotlin",
            "--report", "json",
            "--work-dir", work.path,
            "--startup-timeout", "${STARTUP_TIMEOUT_S}",
        )
        hubs.forEach { command += listOf("--hub", it) }
        only?.let { command += listOf("--only", it) }
        val out = File(work, "report.json")
        val process = ProcessBuilder(command).redirectOutput(out).redirectError(ProcessBuilder.Redirect.INHERIT).start()
        if (!process.waitFor(60, TimeUnit.MINUTES)) {
            process.destroyForcibly()
            fail("the runner did not finish within 60 minutes")
        }
        val report = mapper.readTree(out)
        println("conformance report (exit ${process.exitValue()}, logs in $work):")
        for (c in report.path("checks")) {
            println("  ${c.path("id").asString()} ${c.path("status").asString()} (${c.path("millis").asLong()} ms): ${c.path("detail").asString()}")
        }
        return report
    }

    private fun expect(report: JsonNode): List<String> = report.path("checks").mapNotNull { c ->
        val id = c.path("id").asString()
        val status = c.path("status").asString()
        val expected = if (KNOWN_FAILURES.containsKey(id)) "FAIL" else "PASS"
        if (status == expected) null else "$id $status (expected $expected): ${c.path("detail").asString()}"
    }

    private companion object {
        /** A cold JVM and Spring Boot start in a few seconds; the runner's default (30 s) is kept with a margin. */
        const val STARTUP_TIMEOUT_S = 60

        /** Checks kraken-messaging fails as the runner writes them, with the reason (see the KT-cleanup report). */
        val KNOWN_FAILURES: Map<String, String> = mapOf(
            "L1-STATUS" to "kraken-messaging sends an InstanceStatus every 5 s and also at once whenever its set of live hubs " +
                "changes (documented in its README); the runner requires every gap between two reports on a stream to be 3.5 to " +
                "6.5 s, so the report sent when the second hub comes up (gap 0) fails it. The rule (protocol.md, Instance status: " +
                "\"every 5 s\") does not say whether an extra report is allowed: a contract question, not changed in the library",
        )
    }
}
