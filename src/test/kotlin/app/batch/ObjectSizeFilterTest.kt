package app.batch

import app.domain.InputStreamPair
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.junit.Assert.*
import org.junit.Test
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream


class ObjectSizeFilterTest {

    @Test
    fun test_WhenFileUnderTheMaximumSize_ThenItIsAccepted() {
        val objectSizeFilter = ObjectSizeFilter(100)
        val dataStream = ByteArrayInputStream("DATA".toByteArray())
        val metadataStream = ByteArrayInputStream("METADATA".toByteArray())
        val inputStreamPair = InputStreamPair(dataStream, metadataStream, "S3_KEY", 99)
        val processed = objectSizeFilter.process(inputStreamPair)
        assertNotNull(processed)
        assertEquals(processed, inputStreamPair)
    }

    @Test
    fun test_WhenFileOverTheMaximumSize_ThenItIsRejected() {
        val objectSizeFilter = ObjectSizeFilter(100)
        val dataStream = ByteArrayInputStream("DATA".toByteArray())
        val metadataStream = ByteArrayInputStream("METADATA".toByteArray())
        val inputStreamPair = InputStreamPair(dataStream, metadataStream, "S3_KEY", 101)
        val processed = objectSizeFilter.process(inputStreamPair)
        assertNull(processed)
    }

    @Test
    fun test_WhenFileAtTheMaximumSize_ThenItIsAccepted() {
        val objectSizeFilter = ObjectSizeFilter(100)
        val dataStream = ByteArrayInputStream("DATA".toByteArray())
        val metadataStream = ByteArrayInputStream("METADATA".toByteArray())
        val inputStreamPair = InputStreamPair(dataStream, metadataStream, "S3_KEY", 100)
        val processed = objectSizeFilter.process(inputStreamPair)
        assertNotNull(processed)
        assertEquals(processed, inputStreamPair)
    }

    @Test
    fun test_WhenFileOverTheMaximumSize_ThenItIsLogged() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        val maxSize = 100L
        val objectSizeFilter = ObjectSizeFilter(maxSize)
        val dataStream = ByteArrayInputStream("DATA".toByteArray())
        val metadataStream = ByteArrayInputStream("METADATA".toByteArray())
        val objectSize = 150L
        val s3Key = "S3_KEY"
        val inputStreamPair = InputStreamPair(dataStream, metadataStream, s3Key, objectSize)
        objectSizeFilter.process(inputStreamPair)
        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertEquals(formattedMessages[0], "Rejecting '$s3Key': size - $objectSize greater than the maximum allowed: $maxSize.")
    }
}
