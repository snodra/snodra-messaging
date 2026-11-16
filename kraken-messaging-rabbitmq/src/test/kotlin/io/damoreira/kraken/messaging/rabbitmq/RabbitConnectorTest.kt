package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.Constants
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** What a failed leader lookup says: during a leader election the broker answers "does not exist" for a stream that exists. */
class RabbitConnectorTest {
    @Test
    fun `a stream without a leader the broker can name reads as a possible election, not as a missing stream`() {
        for (code in listOf(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST, Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE)) {
            val message = RabbitConnector.noLeader("kraken.events-1", code)
            assertTrue("kraken.events-1" in message && "no leader" in message && "election" in message && "retried" in message, message)
            assertFalse(message.startsWith("rabbitmq: the stream kraken.events-1: the stream does not exist"), message)
        }
        val refused = RabbitConnector.noLeader("kraken.events-1", Constants.RESPONSE_CODE_ACCESS_REFUSED)
        assertTrue("access refused" in refused, refused)
    }

    @Test
    fun `every other call that meets the same answer says it may be an election too`() {
        for (code in listOf(Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST, Constants.RESPONSE_CODE_STREAM_NOT_AVAILABLE)) {
            val text = RabbitConnector.codeName(code)
            assertTrue("election" in text, text)
        }
    }
}
