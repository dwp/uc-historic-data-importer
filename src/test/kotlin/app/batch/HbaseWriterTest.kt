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
import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.security.Key

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HBaseWriter::class])
@TestPropertySource(properties = [
    "s3.bucket=bucket",
    "hbase.retry.max.attempts=5",
    "hbase.retry.initial.backoff=1",
    "hbase.retry.backoff.multiplier=1",
    "max.batch.size.bytes=100"
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
    private lateinit var hbaseClient: HbaseClient

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

        println("===============================================> hbaseClient: '$hbaseClient'.")
        println("===============================================> messageProducer: '$messageProducer'.")
        doNothing().whenever(hbaseClient).ensureTable("adb:collection")

        //doNothing().`when(hbase).putBatch(topic, key, message1, 100)

        val data = listOf(invalidJson2, validJson)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(7)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Error processing record 1 from '$validFileName': 'parse error'."))
    }

//    @Test
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

        //doNothing().`when`(hbase).putVersion(topic, key, message1, 100)
        doNothing().whenever(manifestWriter).generateManifest(any(), any(), any(), any())

        val data = listOf(invalidJson2, validJsonWithoutId)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(7)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        assertTrue(formattedMessages.contains("Error processing record 1 from '$validFileName': 'parse error'."))

    }

//    @Test
    fun should_Log_Error_And_Retry_10_Times_When_Streaming_Line_Of_File_Fails() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val dataKeyResult = DataKeyResult("", "", "")
        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)

        val inputStream = ByteArrayInputStream("""{ "_id": {"key": "value"}}""".toByteArray())
        val s3InputStream = mock<S3ObjectInputStream>()

        val s3Object = mock<S3Object> {
            on { objectContent } doReturn s3InputStream
        }

        given(s3.getObject("bucket", validFileName)).willReturn(s3Object)
        //whenever(hBaseWriter.getBufferedReader(any())).thenThrow(RuntimeException("wtf"))
        doThrow(RuntimeException("RESET ERROR")).whenever(hBaseWriter).getBufferedReader(any())
        //doNothing().whenever(hbase).putBatch(any())
        doNothing().whenever(manifestWriter).generateManifest(any(), any(), any(), any())
        val byteArray = """{ "_id": {"key": "value"}}""".toByteArray()
        given(cipherService.decompressingDecryptingStream(any(), any(), any())).willReturn(ByteArrayInputStream(byteArray))
        given(messageUtils.parseGson(any())).willReturn(Gson().fromJson("""{ "_id": {"key": "value"}}""", com.google.gson.JsonObject::class.java))
        val key = mock<Key>()

        val inputStreams = mutableListOf(DecompressedStream(inputStream, validFileName, key, ""))
        hBaseWriter.write(inputStreams)
        verify(cipherService, times(10)).decompressingDecryptingStream(any(), any(), any())

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(15)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        assertTrue(formattedMessages.contains("Error on attempt 1 streaming '$validFileName': 'RESET ERROR'."))

        for (i in 2..10) {
            assertTrue(formattedMessages.contains("Error on attempt $i streaming '$validFileName': 'RESET ERROR'."))
        }
    }


//    @Test
    fun testIdObjectReturnedAsObject() {
        val message = com.google.gson.JsonObject()
        val id = com.google.gson.JsonObject()
        id.addProperty("key", "value")
        message.add("_id", id)
        val actual = hBaseWriter.idObject(message)
        val expected = id
        assertEquals(expected, actual)
    }

//    @Test
    fun testIdStringReturnedAsObject() {
        val message = com.google.gson.JsonObject()
        message.addProperty("_id", "id")
        val actual = hBaseWriter.idObject(message)
        val expected = com.google.gson.JsonObject()
        expected.addProperty("id", "id")
        assertEquals(expected, actual)
    }

//    @Test
    fun testIdNumberReturnedAsObject() {
        val message = com.google.gson.JsonObject()
        message.addProperty("_id", 12345)
        val actual = hBaseWriter.idObject(message)
        val expected = com.google.gson.JsonObject()
        expected.addProperty("id", "12345")
        assertEquals(expected, actual)
    }

//    @Test
    fun testIdArrayReturnedAsNull() {
        val message = com.google.gson.JsonObject()
        val arrayValue = com.google.gson.JsonArray()
        arrayValue.add("1")
        arrayValue.add("2")
        message.add("_id", arrayValue)
        val actual = hBaseWriter.idObject(message)
        val expected = null
        assertEquals(expected, actual)
    }

//    @Test
    fun testIdNullReturnedAsNull() {
        val message = com.google.gson.JsonObject()
        val nullValue = com.google.gson.JsonNull.INSTANCE
        message.add("_id", nullValue)
        val actual = hBaseWriter.idObject(message)
        val expected = null
        assertEquals(expected, actual)

    }

//    @Test
    fun testPutBatchRetries() {
        try {
//            given(hbase.putBatch(any())).willThrow(java.lang.RuntimeException("Failed to put batch"))
//            val record = HBaseRecord("topic".toByteArray(), "key".toByteArray(), "body".toByteArray(), 1.toLong())
//            hBaseWriter.putBatch(listOf(record))
        }
        catch (e: Exception) {
//            verify(hbase, times(5)).putBatch(any())
        }
    }

//    @Test
    fun testMaxBatchSize() {
        val byteArrayOutputStream = ByteArrayOutputStream()

        val json = """{
            |   "_id": { "id": "id" },
            |    "key1": "value1",
            |    "key2": "value2"
            |}""".trimMargin().replace("\n", " ")

        for (i in 1..100) {
            byteArrayOutputStream.write("$json\n".toByteArray())
        }

        byteArrayOutputStream.close()
        val byteArray = byteArrayOutputStream.toByteArray()
        val items = mutableListOf(DecompressedStream(ByteArrayInputStream(byteArray),
            "database.collection.0001.json.gz.enc", mock<Key>(), "AAAAAAAAAAAAAAAAAAAAAA=="))

        given(messageUtils.parseGson(any())).willReturn(Gson().fromJson(json, com.google.gson.JsonObject::class.java))
        whenever(keyService.batchDataKey()).thenReturn(DataKeyResult("", "", ""))
        given(cipherService.encrypt(any(), any())).willReturn(EncryptionResult("AAAAAAAAAAAAAAAAAAAAAA==", "qwertyuiop"))
        given(messageProducer.produceMessage(any(), any(), any(), any(), any(), any())).willReturn("""{ "message": $json """)
        given(messageUtils.getLastModifiedTimestamp(any())).willReturn("1980-01-01T00:00:00.000Z")
        given(messageUtils.parseJson(any())).willReturn(JsonObject(mapOf(Pair("key", "value"))))
        given(messageUtils.generateKeyFromRecordBody(any())).willReturn("FORMATTED_KEY".toByteArray())
        hBaseWriter.write(items)
        //verify(hBaseWriter, times(100)).putBatch(any())
    }

    private fun getInputStream(data1: List<String>, fileName: String): DecompressedStream {
        val baos = ByteArrayOutputStream()
        data1.forEach {
            val nl = it + "\n"
            baos.write(nl.toByteArray())
        }
        val inputStream = ByteArrayInputStream(baos.toByteArray())
        val key = mock<Key>()
        return DecompressedStream(inputStream, fileName, key, "")
    }
}


