package app.batch

import app.domain.DecompressedStream
import app.domain.DecryptedStream
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.IOUtils
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.security.Key


class DecompressionProcessorTest {

    private val fileName = "test.json.gz"

    @Test
    fun should_Decompress_Correctly_When_Given_Gzipped_Stream() {

        val (data, decompressed) = decompress(CompressorStreamFactory.GZIP)

        val gzippedStream = decompressed?.inputStream as GzipCompressorInputStream
        assertEquals(IOUtils.toString(gzippedStream, StandardCharsets.UTF_8), data)
        assertEquals(0, data.length.compareTo(gzippedStream.uncompressedCount))
    }

    @Test(expected = RuntimeException::class)
    fun should_Throw_Exception_When_Given_Non_Gzipped_Stream() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        decompress(CompressorStreamFactory.BZIP2)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Exception occurred when decompressing the gzip decrypted input stream from the file $fileName"))
    }

    @Test
    fun should_Log_Compressed_Count_When_Given_Gzipped_Stream() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        val (_, decompressed) = decompress(CompressorStreamFactory.GZIP)
        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(2)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        val gzippedStream = decompressed?.inputStream as GzipCompressorInputStream
        assertTrue(formattedMessages.contains("Compressed file\", \"file_name\":\"$fileName\", \"file_size\":\"${gzippedStream.compressedCount}"))
    }

    @Test(expected = RuntimeException::class)
    fun should_Log_Exception_Msg_When_Given_Non_Gzipped_Stream() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        decompress(CompressorStreamFactory.BZIP2)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Exception occurred when decompressing the gzip decrypted input stream from the file $fileName"))
    }

    private fun decompress(format: String): Pair<String, DecompressedStream?> {
        val data = "dataworksdataworksdataworksdataworksdataworksdataworksdataworks"
        val byteArray = data.toByteArray(StandardCharsets.UTF_8)
        val testOutputStream = ByteArrayOutputStream()
        val zippedStream = CompressorStreamFactory().createCompressorOutputStream(format,
            testOutputStream)
        zippedStream.write(byteArray)
        zippedStream.close()

        val inputStream = ByteArrayInputStream(testOutputStream.toByteArray())
        val key = mock<Key>()
        val decryptedStream = DecryptedStream(inputStream, fileName, key, "")
        val decompressionProcessor = DecompressionProcessor()
        val decompressed = decompressionProcessor.process(decryptedStream)
        return Pair(data, decompressed)
    }

}
