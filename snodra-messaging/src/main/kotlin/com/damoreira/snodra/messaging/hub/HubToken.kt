package com.damoreira.snodra.messaging.hub

import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * The static hub token a service sends in `Register.credentials.token`, or none.
 *
 * [current] is asked once per stream, so a token that changes between reconnects (a rotated container secret) is picked
 * up by the next attempt. Surrounding whitespace is dropped (a secret file ends in a newline) and an empty inline token is no
 * token; a token file must hold one at startup and keeps the last good token when it turns empty later (see [of]). The value is never logged and [toString] never shows it.
 */
class HubToken(private val source: () -> String?) {
    /** The token to send now, or null for none. */
    fun current(): String? = source()?.trim()?.takeIf { it.isNotEmpty() }

    /** Redacted: a [HubSettings] or a properties object holding this prints no secret. */
    override fun toString(): String = "HubToken(****)"

    companion object {
        /** No token: `Register` carries no credentials. */
        val NONE = HubToken { null }

        /**
         * The token of `snodra.messaging.hub.token` / `token-file`: when [file] is set it is the only source and wins over
         * [inline] (it is the one that can rotate), read now and again at every [HubToken.current]; otherwise [inline]. Later, an
         * empty (or unreadable) file keeps the last token and logs one WARN.
         *
         * @throws IllegalStateException if [file] cannot be read now, or holds nothing but whitespace (a configured secret that
         *   is not there is a deployment error, better at startup than as a stream of rejections)
         */
        fun of(inline: String?, file: Path?): HubToken =
            if (file != null) FileSource(file).also { it.readOrThrow() }.let { HubToken(it::read) }
            else if (inline.isNullOrBlank()) NONE
            else HubToken { inline }
    }

    private class FileSource(private val path: Path) {
        private val log = LoggerFactory.getLogger(HubToken::class.java)
        @Volatile private var lastGood: String? = null
        @Volatile private var lastProblem: String? = null

        fun readOrThrow() {
            val content = try {
                Files.readString(path)
            } catch (e: IOException) {
                throw IllegalStateException("snodra.messaging.hub.token-file $path cannot be read: ${e.javaClass.simpleName}", e)
            }
            check(content.isNotBlank()) { "snodra.messaging.hub.token-file $path is empty: it must hold the hub token" }
            lastGood = content
        }

        /**
         * The file's content now; the last good content when it cannot be read or is empty (a rotation may replace the
         * file non-atomically, and a Register without a token would be refused and wait for the back-off cap).
         */
        fun read(): String? = try {
            val content = Files.readString(path)
            if (content.isBlank()) {
                problem("empty")
                lastGood
            } else {
                lastGood = content
                lastProblem = null
                content
            }
        } catch (e: IOException) {
            problem(e.javaClass.simpleName)
            lastGood
        }

        private fun problem(problem: String) {
            if (problem != lastProblem) {
                lastProblem = problem
                log.warn("Hub token file {} cannot be read ({}); using the last token read from it", path, problem)
            }
        }
    }
}
