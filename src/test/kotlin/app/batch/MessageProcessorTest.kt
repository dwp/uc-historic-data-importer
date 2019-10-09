package app.batch

import app.configuration.HttpClientProvider
import app.services.CipherService
import app.services.KeyService
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.nhaarman.mockitokotlin2.*
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
class MessageProcessorTest {
    @MockBean
    private lateinit var httpClientProvider: HttpClientProvider

    @MockBean
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var cipherService: CipherService

    @Test
    fun Should_Log_Error_When_Not_Valid_Json() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val dataKeyResult = DataKeyResult("", "", "")
        doNothing().whenever(keyService).batchDataKey()
        val encryptionResult = EncryptionResult("", "'")
        doNothing().whenever(cipherService).encrypt("","".toByteArray())
        val inputStream = ByteArrayInputStream("dataworks".toByteArray())
        val messageProcessor = MessageProcessor()
        messageProcessor.process(inputStream)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Error while parsing json:"))

    }


}
