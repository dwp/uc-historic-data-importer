package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.beust.klaxon.JsonObject
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HBaseWriter::class])
@TestPropertySource(properties = [
    "s3.bucket=bucket"
])
class HbaseWriterTest {

    val validJsonWithoutTimeStamp = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"}""".trimIndent()
    val validJsonWithoutId = """{"type":"addressDeclaration"}""".trimIndent()
    val invalidJson2 = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"""".trimIndent()
    val validJson = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"}""".trimIndent()
    val validFileName = "adb.collection.0001.json.gz.enc"

    @MockBean
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var s3: AmazonS3

    @MockBean
    private lateinit var cipherService: CipherService

    @MockBean
    private lateinit var hbase: HbaseClient

    @MockBean
    private lateinit var messageProducer: MessageProducer

    @MockBean
    private lateinit var messageUtils: MessageUtils

    @MockBean
    private lateinit var manifestWriter: ManifestWriter

    @SpyBean
    private lateinit var hBaseWriter: HBaseWriter

    @Test
    fun should_Log_Error_For_Invalid_Json_And_continue() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val dataKeyResult = DataKeyResult("", "", "")
        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)
        val encryptionResult = EncryptionResult("", "")
        whenever(cipherService.encrypt(any(), any())).thenReturn(encryptionResult)

        whenever(messageUtils.parseGson(invalidJson2)).thenThrow(RuntimeException("parse error"))
        val jsonObject = JsonObject()
        whenever(messageUtils.parseJson(validJson)).thenReturn(jsonObject)
        whenever(messageUtils.getId(jsonObject)).thenReturn(jsonObject)

        whenever(messageUtils.getLastModifiedTimestamp(jsonObject)).thenReturn("something")
        whenever(messageUtils.getTimestampAsLong("")).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(com.google.gson.JsonObject(), """{ "key": "value" }""", encryptionResult, dataKeyResult, "adb", "collection")).thenReturn(message)

        val formattedKey = "0000-0000-00001"
        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        doNothing().whenever(manifestWriter).generateManifest(any(), any(), any(), any())

        val topic = "adb.collection".toByteArray()
        val key = formattedKey.toByteArray()
        val message1 = message.toByteArray()

        doNothing().`when`(hbase).putVersion(topic, key, message1, 100)

        val data = listOf(invalidJson2, validJson)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(6)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Error processing record 1 from '$validFileName': 'parse error'."))
    }

    @Test
    fun should_Log_Error_For_Json_Without_Id() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val dataKeyResult = DataKeyResult("", "", "")
        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)
        val encryptionResult = EncryptionResult("", "")
        whenever(cipherService.encrypt(any(), any())).thenReturn(encryptionResult)

        whenever(messageUtils.parseGson(invalidJson2)).thenThrow(RuntimeException("parse error"))
        val jsonObject = JsonObject()
        whenever(messageUtils.parseGson(validJsonWithoutId)).thenReturn(com.google.gson.JsonObject())
        whenever(messageUtils.getIdFromDbObject(jsonObject)).thenReturn(null)

        whenever(messageUtils.getLastModifiedTimestamp(jsonObject)).thenReturn("")
        whenever(messageUtils.getTimestampAsLong("")).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(com.google.gson.JsonObject(), """{"key": "value"}""",
                encryptionResult, dataKeyResult, "adb", "collection")).thenReturn(message)
        val formattedKey = "0000-0000-00001"

        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        val topic = "adb.collection".toByteArray()
        val key = formattedKey.toByteArray()
        val message1 = message.toByteArray()

        doNothing().`when`(hbase).putVersion(topic, key, message1, 100)
        doNothing().whenever(manifestWriter).generateManifest(any(), any(), any(), any())

        val data = listOf(invalidJson2, validJsonWithoutId)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(6)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        assertTrue(formattedMessages.contains("Error processing record 1 from '$validFileName': 'parse error'."))

    }

    @Test
    fun should_Log_Error_And_Retry_10_Times_When_Streaming_Line_Of_File_Fails() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val dataKeyResult = DataKeyResult("", "", "")
        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)

        val inputStream = ByteArrayInputStream(ByteArray(0))
        val s3InputStream = mock<S3ObjectInputStream>()

        val s3Object = mock<S3Object>() {
            on { objectContent } doReturn s3InputStream
        }

        given(s3.getObject("bucket", validFileName)).willReturn(s3Object)
        whenever(hBaseWriter.getBufferedReader(inputStream)).thenReturn(null)
        doNothing().whenever(hbase).putBatch(any())
        doNothing().whenever(manifestWriter).generateManifest(any(), any(), any(), any())

        val inputStreams = mutableListOf(DecompressedStream(inputStream, validFileName))
        hBaseWriter.write(inputStreams)
        verify(s3, times(10)).getObject("bucket", validFileName)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(14)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        assertTrue(formattedMessages.contains("Error on attempt 1 streaming '$validFileName': 'Parameter specified as non-null is null: method kotlin.io.TextStreamsKt.forEachLine, parameter receiver\$0'."))

        for (i in 2..10) {
            assertTrue(formattedMessages.contains("Error on attempt $i streaming '$validFileName': 'Underlying input stream returned zero bytes'."))
        }
    }

    @Test
    fun should_Retry_Until_Success_When_Streaming_Fails() {

        val dataKeyResult = DataKeyResult("", "", "")
        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)

        val inputStream = ByteArrayInputStream(ByteArray(0))
        val s3InputStream = mock<S3ObjectInputStream>()

        val s3Object = mock<S3Object>() {
            on { objectContent } doReturn s3InputStream
        }

        given(s3.getObject(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).willReturn(s3Object)

        whenever(hBaseWriter.getBufferedReader(inputStream))
                .thenReturn(null)

        doNothing().whenever(hbase).putBatch(any())
        doNothing().whenever(manifestWriter).generateManifest(any(), any(), any(), any())

        val inputStreams = mutableListOf(DecompressedStream(inputStream, validFileName))
        hBaseWriter.write(inputStreams)
    }

    private fun getInputStream(data1: List<String>, fileName: String): DecompressedStream {
        val baos = ByteArrayOutputStream()
        data1.forEach {
            val nl = it + "\n"
            baos.write(nl.toByteArray())
        }
        val inputStream = ByteArrayInputStream(baos.toByteArray())
        return DecompressedStream(inputStream, fileName)
    }
}


