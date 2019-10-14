package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.beust.klaxon.JsonObject
import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.client.Connection
import org.junit.Assert.assertTrue
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.lang.RuntimeException
import javax.annotation.PostConstruct

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HBaseWriter::class])

class HbaseWriterTest {

    val validJsonWithoutTimeStamp = """{"type":"addressDeclaration"}""".trimIndent()
    val validJsonWithoutId = """{"type":"addressDeclaration"}""".trimIndent()
    val invalidJson2 = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"""".trimIndent()
    val validJson = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"}""".trimIndent()
    val validFileName = "adb.collection.0001.json.gz.enc"

    @MockBean
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var cipherService: CipherService

    @MockBean
    private lateinit var hbase: HbaseClient

    @MockBean
    private lateinit var messageProducer: MessageProducer

    @MockBean
    private lateinit var messageUtils: MessageUtils

    @MockBean
    private lateinit var connection: Connection

    @Autowired
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

        whenever(messageUtils.parseJson(invalidJson2)).thenThrow(RuntimeException("parse error"))
        val jsonObject = JsonObject()
        whenever(messageUtils.parseJson(validJson)).thenReturn(jsonObject)
        whenever(messageUtils.getId(jsonObject)).thenReturn(jsonObject)

        whenever(messageUtils.getLastModifiedTimestamp(jsonObject)).thenReturn("something")
        whenever(messageUtils.getTimestampAsLong("")).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(jsonObject, encryptionResult, dataKeyResult, "adb", "collection")).thenReturn(message)

        val formattedKey = "0000-0000-00001"
        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        val topic = "adb.collection".toByteArray()
        val key = formattedKey.toByteArray()
        val message1 = message.toByteArray()

       doNothing().`when`(hbase).putVersion(topic,key,message1,100)

        val data = listOf(invalidJson2, validJson)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(4)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        assertTrue(formattedMessages.contains("Error while parsing record from '$validFileName': 'parse error'."))
        //verify(hbase, times(1)).putVersion(topic,key,message1,100)

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

        whenever(messageUtils.parseJson(invalidJson2)).thenThrow(RuntimeException("parse error"))
        val jsonObject = JsonObject()
        whenever(messageUtils.parseJson(validJsonWithoutId)).thenReturn(jsonObject)
        whenever(messageUtils.getId(jsonObject)).thenReturn(null)

        whenever(messageUtils.getLastModifiedTimestamp(jsonObject)).thenReturn("")
        whenever(messageUtils.getTimestampAsLong("")).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(jsonObject, encryptionResult, dataKeyResult, "adb", "collection")).thenReturn(message)

        val formattedKey = "0000-0000-00001"
        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        val topic = "adb.collection".toByteArray()
        val key = formattedKey.toByteArray()
        val message1 = message.toByteArray()

        doNothing().`when`(hbase).putVersion(topic,key,message1,100)

        val data = listOf(invalidJson2, validJsonWithoutId)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(4)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        assertTrue(formattedMessages.contains("Error while parsing record from '$validFileName': 'parse error'."))
        assertTrue(formattedMessages.contains("Skipping record 2 in the file $validFileName due to absence of id"))

    }

      @Test
     fun should_Log_Error_For_Json_Without_Timestamp() {

         val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
         val mockAppender: Appender<ILoggingEvent> = mock()
         root.addAppender(mockAppender)

         val dataKeyResult = DataKeyResult("", "", "")
         whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)
         val encryptionResult = EncryptionResult("", "")
         whenever(cipherService.encrypt(any(), any())).thenReturn(encryptionResult)

         whenever(messageUtils.parseJson(invalidJson2)).thenThrow(RuntimeException("parse error"))
         val jsonObject = JsonObject()
         whenever(messageUtils.parseJson(validJsonWithoutTimeStamp)).thenReturn(jsonObject)
         whenever(messageUtils.getId(jsonObject)).thenReturn(jsonObject)

         whenever(messageUtils.getLastModifiedTimestamp(jsonObject)).thenReturn(null)
         //whenever(messageUtils.getTimestampAsLong("")).thenReturn(100)
         val message = "message"
         whenever(messageProducer.produceMessage(jsonObject, encryptionResult, dataKeyResult, "adb", "collection")).thenReturn(message)

         val formattedKey = "0000-0000-00001"
         whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

         val topic = "adb.collection".toByteArray()
         val key = formattedKey.toByteArray()
         val message1 = message.toByteArray()

         doNothing().`when`(hbase).putVersion(topic,key,message1,100)

         val data = listOf(invalidJson2, validJsonWithoutTimeStamp)
         val inputStreams = mutableListOf(getInputStream(data, validFileName))
         hBaseWriter.write(inputStreams)

         val captor = argumentCaptor<ILoggingEvent>()
         verify(mockAppender, times(4)).doAppend(captor.capture())
         val formattedMessages = captor.allValues.map { it.formattedMessage }

         assertTrue(formattedMessages.contains("Error while parsing record from '$validFileName': 'parse error'."))
         assertTrue(formattedMessages.contains("Skipping record 2 in the file $validFileName due to absence of lastModifiedTimeStamp"))

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
