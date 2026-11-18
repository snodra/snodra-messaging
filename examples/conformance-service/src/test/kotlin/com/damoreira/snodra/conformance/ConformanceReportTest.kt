package com.damoreira.snodra.conformance

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** A run that leaves no usable report says so, with the runner's exit code and where its logs are. */
class ConformanceReportTest {
    @Test
    fun `a missing, empty or unreadable report or one without checks fails clearly`(@TempDir work: File) {
        val out = File(work, "report.json")
        for (content in listOf(null, "", "not json", """{"checks": []}""", """{"summary": "x"}""")) {
            if (content == null) out.delete() else out.writeText(content)
            val e = assertFailsWith<AssertionError>("$content") { ConformanceReport.read(out, exitValue = 2, work = work) }
            assertTrue("exit 2" in e.message!! && work.path in e.message!!, "$content: ${e.message}")
            assertTrue("report" in e.message!!, e.message)
        }
    }

    @Test
    fun `a report with checks is read`(@TempDir work: File) {
        val out = File(work, "report.json").apply { writeText("""{"checks": [{"id": "L1-X", "status": "PASS"}]}""") }
        assertEquals("L1-X", ConformanceReport.read(out, exitValue = 0, work = work).path("checks").first().path("id").asString())
    }
}
