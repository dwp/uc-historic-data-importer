package app.batch

import app.configuration.HttpClientProvider
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
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

@RunWith(SpringRunner::class)
@ActiveProfiles("httpDataKeyService", "awsS3")
@SpringBootTest
@TestPropertySource(properties = [
    "hbase.zookeeper.quorum=hbase",
    "aws.region=eu-west-1",
    "s3.bucket=bucket1",
    "s3.prefix.folder=test/output/",
    "s3.key.regex=([A-Za-z]*\\.[A-Za-z]*\\.[0-9]{4}\\.json\\.gz)",
    "s3.data.key.extension=\\.enc$",
    "s3.metadata.key.extension=\\.encryption\\.json$"
])
class DecompressionProcessorTest {
    @MockBean
    private lateinit var httpClientProvider: HttpClientProvider

    @Test
    fun Should_Decompress_Correctly_When_Given_Gzipped_Stream() {

        val data = "dataworksdataworksdataworksdataworksdataworksdataworksdataworks"
        val byteArray = data.toByteArray(StandardCharsets.UTF_8)
        val testOutputStream = ByteArrayOutputStream()
        val gzippedStream = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP,
            testOutputStream)
        gzippedStream.write(byteArray)
        gzippedStream.close()

        val inputStream = ByteArrayInputStream(testOutputStream.toByteArray())
        val decryptedStream = DecryptedStream(inputStream, "test.json.gz")
        val decompressionProcessor = DecompressionProcessor()
        val decompressed = decompressionProcessor.process(decryptedStream) as GzipCompressorInputStream

        assertEquals(IOUtils.toString(decompressed, StandardCharsets.UTF_8), data)
        assertEquals(0, data.length.compareTo(decompressed.uncompressedCount))
    }

    @Test(expected = RuntimeException::class)
    fun Should_Throw_Exception_When_Given_Non_Gzipped_Stream() {
        val data = "dataworksdataworksdataworksdataworksdataworksdataworksdataworks"
        val byteArray = data.toByteArray(StandardCharsets.UTF_8)
        val testOutputStream = ByteArrayOutputStream()
        val bzippedStream = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,
            testOutputStream)
        bzippedStream.write(byteArray)
        bzippedStream.close()

        val inputStream = ByteArrayInputStream(testOutputStream.toByteArray())
        val decryptedStream = DecryptedStream(inputStream, "test.json.bz2")
        val decompressionProcessor = DecompressionProcessor()
        decompressionProcessor.process(decryptedStream)
    }

    @Test
    fun Should__Log_Compressed_Count_When_Given_Gzipped_Stream() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val data = "dataworksdataworksdataworksdataworksdataworksdataworksdataworks"
        val byteArray = data.toByteArray(StandardCharsets.UTF_8)
        val testOutputStream = ByteArrayOutputStream()
        val bzippedStream = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.GZIP,
            testOutputStream)
        bzippedStream.write(byteArray)
        bzippedStream.close()

        val inputStream = ByteArrayInputStream(testOutputStream.toByteArray())
        val fileName = "test.json.gz"
        val decryptedStream = DecryptedStream(inputStream, fileName)
        val decompressionProcessor = DecompressionProcessor()
        val decompressed = decompressionProcessor.process(decryptedStream) as GzipCompressorInputStream
        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Compressed size of the file $fileName : ${decompressed.compressedCount}"))
    }

    @Test(expected = RuntimeException::class)
    fun Should_Log_Exception_Msg_When_Given_Non_Gzipped_Stream() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val data = "dataworksdataworksdataworksdataworksdataworksdataworksdataworks"
        val byteArray = data.toByteArray(StandardCharsets.UTF_8)
        val testOutputStream = ByteArrayOutputStream()
        val bzippedStream = CompressorStreamFactory().createCompressorOutputStream(CompressorStreamFactory.BZIP2,
            testOutputStream)
        bzippedStream.write(byteArray)
        bzippedStream.close()

        val inputStream = ByteArrayInputStream(testOutputStream.toByteArray())
        val fileName = "test.json.gz"
        val decryptedStream = DecryptedStream(inputStream, fileName)
        val decompressionProcessor = DecompressionProcessor()
        decompressionProcessor.process(decryptedStream)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Exception occurred when decompressing the gzip decrypted input stream from the file $fileName"))
    }
}
