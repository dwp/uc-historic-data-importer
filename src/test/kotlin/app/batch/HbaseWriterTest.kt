package app.batch

import app.configuration.HttpClientProvider
import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.client.Connection
import org.junit.Assert
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

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
    "s3.metadata.key.extension=\\.encryption\\.json$",
    "data.key.service.url=phoney"
])
class HbaseWriterTest {

    val invalidJson1 = """
            {"_id":{"declarationId":"47a4fad9-49af-4cb2-91b0-0056e2ac0eef"},
            "type":"addressDeclaration"
        """.trimIndent()
    val invalidJson2 = """
            {
            "type":"addressDeclaration"
            }
        """.trimIndent()
    val validJson = """
            {"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},
            "type":"addressDeclaration"
            }
        """.trimIndent()
    val fileName1 = "file1"

    @MockBean
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var cipherService: CipherService

    @MockBean
    private lateinit var httpClientProvider: HttpClientProvider

    @MockBean
    private lateinit var connection: Connection

    @Autowired
    private lateinit var hBaseWriter: HBaseWriter

    @Test
    fun should_Log_And_Continue_When_DBObject_IsNot_Valid_Json() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        whenever(keyService.batchDataKey()).thenReturn(DataKeyResult("","",""))
        whenever(cipherService.encrypt(any(), any())).thenReturn(EncryptionResult("",""))
        val data = listOf(invalidJson1,invalidJson2, validJson)
        val inputStreams = mutableListOf(getInputStream(data, fileName1))
        hBaseWriter.write(inputStreams)
        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(2)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Parsing DB object of id {\"declarationId\":\"87a4fad9-49af-4cb2-91b0-0056e2ac0eef\"} in the file file1"))
        assertTrue(formattedMessages.contains("DB object  does not contain _id field"))
        //assertTrue(formattedMessages.contains("Error while parsing the file file1: "))
    }

    private fun getInputStream(data1: List<String>, fileName: String): DecompressedStream {
        val baos = ByteArrayOutputStream()
        data1.forEach {
            baos.write(it.toByteArray())
        }
        val inputStream = ByteArrayInputStream(baos.toByteArray())
        return DecompressedStream(inputStream, fileName)
    }
}
