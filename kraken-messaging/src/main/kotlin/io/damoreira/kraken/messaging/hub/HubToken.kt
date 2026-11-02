package io.damoreira.kraken.messaging.hub

import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path

/**
 * The static hub token (P5a, ADR 0018) a service sends in `Register.credentials.token`, or none.
 *
 * [current] is asked once per stream, so a token that changes between reconnects (a rotated container secret) is picked
 * up by the next attempt. Surrounding whitespace is dropped (a secret file ends in a newline) and an empty token is no
 * token. The value is never logged and [toString] never shows it.
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
         * The token of `kraken.messaging.hub.token` / `token-file`: when [file] is set it is the only source and wins over
         * [inline] (it is the one that can rotate), read now and again at every [HubToken.current]; otherwise [inline].
         *
         * @throws IllegalStateException if [file] cannot be read now (a configured secret that is not there is a
         *   deployment error, better at startup than as a stream of rejections)
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
            try {
                lastGood = Files.readString(path)
            } catch (e: IOException) {
                throw IllegalStateException("kraken.messaging.hub.token-file $path cannot be read: ${e.javaClass.simpleName}", e)
            }
        }

        /** The file's content now; the last good content when it cannot be read (a rotation may replace it non-atomically). */
        fun read(): String? = try {
            Files.readString(path).also { lastGood = it; lastProblem = null }
        } catch (e: IOException) {
            val problem = e.javaClass.simpleName
            if (problem != lastProblem) {
                lastProblem = problem
                log.warn("Hub token file {} cannot be read ({}); using the last token read from it", path, problem)
            }
            lastGood
        }
    }
}
