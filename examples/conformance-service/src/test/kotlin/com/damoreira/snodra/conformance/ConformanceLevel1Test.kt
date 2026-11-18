package com.damoreira.snodra.conformance

import com.damoreira.snodra.messaging.test.SnodraContainers
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.Test
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.File
import java.util.concurrent.TimeUnit
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * The Kotlin conformance service against the conformance runner (`snodra-conformance`, snodra-server `conformance/`): every check
 * of level 1 on one real hub (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`) and the runner's fake hubs, then the two-hub part of
 * `L1-HUBS` on two. Needs Docker and the runner binary: `SNODRA_CONFORMANCE_BIN=/path/to/snodra-conformance ./gradlew
 * :examples:conformance-service:test`; without the binary the test is skipped, saying so.
 *
 * It asserts exactly the known failures of this library ([KNOWN_FAILURES]), so a gap stays visible and a fix (or a new failure)
 * changes the test.
 */
class ConformanceLevel1Test {
    @Test
    fun kotlin_reference_passes_level_1() {
        val runner = System.getProperty("conformance.runner")?.let(::File)
        assumeTrue(
            runner != null && runner.canExecute(),
            "the conformance runner is not available: set SNODRA_CONFORMANCE_BIN to the snodra-conformance binary " +
                "(cargo build --release -p snodra-conformance in snodra-server/conformance)",
        )
        assumeTrue(SnodraContainers.hubImageAvailable(), "the hub image ${System.getenv("SNODRA_SERVER_IMAGE") ?: "snodra-server:dev"} is not available locally")
        val jar = File(System.getProperty("conformance.jar"))
        val hub1 = SnodraContainers.hub(emptyMap()).also { it.start() }
        val wrong = mutableListOf<String>()
        try {
            val a = "127.0.0.1:${hub1.getMappedPort(7000)}"
            // One hub: every check (L1-HUBS on fake hubs only).
            val report = run(runner!!, jar, listOf(a), only = null)
            wrong += expect(report)
            // Two hubs: L1-HUBS with its part on two real hubs.
            val hub2 = SnodraContainers.hub(emptyMap()).also { it.start() }
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
        val report = ConformanceReport.read(out, process.exitValue(), work)
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

        /**
         * Checks snodra-messaging fails as the runner writes them, with the reason: none. (L1-STATUS failed until the runner
         * allowed the extra report sent when the set of live hubs changes: the contract bounds only the longest gap.)
         */
        val KNOWN_FAILURES: Map<String, String> = emptyMap()
    }
}

/** The runner's JSON report. */
internal object ConformanceReport {
    private val mapper = JsonMapper.builder().build()

    /** The report in [out] the runner wrote; a missing, empty or unreadable one, or one without checks, fails naming [exitValue] and [work]. */
    fun read(out: File, exitValue: Int, work: File): JsonNode {
        fun noReport(why: String): Nothing =
            fail("the runner (exit $exitValue) left $why at $out: its own errors are above, the service's logs in $work")
        if (!out.isFile || out.length() == 0L) noReport("no report")
        val report = try {
            mapper.readTree(out)
        } catch (e: Exception) {
            noReport("a report that is not JSON (${e.message})")
        }
        if (report.path("checks").isEmpty) noReport("a report without checks")
        return report
    }
}
