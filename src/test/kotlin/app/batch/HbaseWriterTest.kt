package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.domain.HBaseRecord
import app.services.CipherService
import app.services.KeyService
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.beust.klaxon.JsonObject
import com.google.gson.Gson
import com.google.gson.JsonPrimitive
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

    val validJsonWithoutId = """{"type":"addressDeclaration"}""".trimIndent()
    val invalidJson2 = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"""".trimIndent()
    val validJson = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"}""".trimIndent()
    val validFileName = "adb.collection.0001.json.gz.enc"
    val EPOCH = "1980-01-01T00:00:00.000Z"

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

        whenever(messageUtils.getTimestampAsLong(any())).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(com.google.gson.JsonObject(), """{ "key": "value" }""", 
                false, false, """{ "key": "value" }""", false, false, false, encryptionResult, dataKeyResult, "adb",
                "collection")).thenReturn(message)

        val formattedKey = "0000-0000-00001"
        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        doNothing().whenever(hBaseWriter).ensureTable("adb:collection")
        doNothing().whenever(hBaseWriter).putBatch(any(), any())

        val data = listOf(invalidJson2, validJson)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(7)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Error processing record\", \"line_number\":\"1\", \"file_name\":\"adb.collection.0001.json.gz.enc\", \"error_message\":\"parse error"))
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
        whenever(messageUtils.getId(jsonObject)).thenReturn(null)
        whenever(messageUtils.getId(jsonObject)).thenReturn(null)

        whenever(messageUtils.getTimestampAsLong(any())).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(com.google.gson.JsonObject(), """{"key": "value"}""", 
                false, false, """{ "key": "value" }""", false, false, false, encryptionResult, dataKeyResult, "adb",
                "collection")).thenReturn(message)
        val formattedKey = "0000-0000-00001"

        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        doNothing().whenever(hBaseWriter).ensureTable("adb:collection")
        doNothing().whenever(hBaseWriter).putBatch(any(), any())

        val data = listOf(invalidJson2, validJsonWithoutId)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(7)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        assertTrue(formattedMessages.contains("Error processing record\", \"line_number\":\"1\", \"file_name\":\"adb.collection.0001.json.gz.enc\", \"error_message\":\"parse error"))
    }

    @Test
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
        doNothing().whenever(hBaseWriter).ensureTable("adb:collection")
        doNothing().whenever(hBaseWriter).putBatch(any(), any())
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

        assertTrue(formattedMessages.contains("Error streaming file\", \"attempt_number\":\"1\", \"file_name\":\"$validFileName\", \"error_message\":\"RESET ERROR"))

        for (i in 2..10) {
            assertTrue(formattedMessages.contains("Error streaming file\", \"attempt_number\":\"$i\", \"file_name\":\"$validFileName\", \"error_message\":\"RESET ERROR"))
        }
    }


    @Test
    fun testIdObjectReturnedAsObject() {
        val id = com.google.gson.JsonObject()
        id.addProperty("key", "value")
        val expectedId = Gson().toJson(id)
        val expectedModified = "UNMODIFIED_ID_OBJECT"
        val (actualId, actualModified) = hBaseWriter.id(Gson(), id)
        assertEquals(expectedId, actualId)
        assertEquals(actualModified, expectedModified)
    }

    @Test
    fun testIdStringReturnedAsString() {
        val id = JsonPrimitive("id")
        val actual = hBaseWriter.id(Gson(), id)
        assertEquals(Pair("id", "UNMODIFIED_ID_STRING"), actual)
    }

    @Test
    fun testMongoIdStringReturnedAsString() {
        val oid = com.google.gson.JsonObject()
        val oidValue = "OID_VALUE"
        oid.addProperty("\$oid", oidValue)
        val actual = hBaseWriter.id(Gson(), oid)
        assertEquals(Pair(oidValue, "MODIFIED_ID"), actual)
    }

    @Test
    fun testIdNumberReturnedAsObject() {
        val id = JsonPrimitive( 12345)
        val actual = hBaseWriter.id(Gson(), id)
        val expectedId = "12345"
        val expectedModified = "UNMODIFIED_ID_STRING"
        assertEquals(Pair(expectedId, expectedModified), actual)
    }

    @Test
    fun testIdArrayReturnedAsNull() {
        val arrayValue = com.google.gson.JsonArray()
        arrayValue.add("1")
        arrayValue.add("2")
        val actual = hBaseWriter.id(Gson(), arrayValue)
        val expected = Pair("", "MODIFIED_ID")
        assertEquals(expected, actual)
    }

    @Test
    fun testIdNullReturnedAsEmpty() {
        val nullValue = com.google.gson.JsonNull.INSTANCE
        val actual = hBaseWriter.id(Gson(), nullValue)
        val expected = Pair("", "MODIFIED_ID")
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeAsNonDateObjectReturnedAsCreated() {
        val lastModified = com.google.gson.JsonObject()
        val lastModifiedValue = "testDateField"
        lastModified.addProperty("\$notDate", lastModifiedValue)
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), lastModified, "CREATED_TIMESTAMP")
        val expected = Pair("CREATED_TIMESTAMP", true)
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeAsDateObjectReturnedAsDateFieldValue() {
        val lastModified = com.google.gson.JsonObject()
        val lastModifiedValue = "testDateField"
        lastModified.addProperty("\$date", lastModifiedValue)
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), lastModified, "CREATED_TIMESTAMP")
        val expected = Pair(lastModifiedValue, true)
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeAsStringReturnedAsValue() {
        val lastModified = JsonPrimitive("testDateString")
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), lastModified, "CREATED_TIMESTAMP")
        val expected = Pair("testDateString", false)
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeArrayReturnedAsCreatedWhenCreatedNotBlank() {
        val arrayValue = com.google.gson.JsonArray()
        arrayValue.add("1")
        arrayValue.add("2")
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), arrayValue, "")
        val expected = Pair(EPOCH, true)
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeNullReturnedAsCreatedWhenCreatedNotBlank() {
        val nullValue = com.google.gson.JsonNull.INSTANCE
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), nullValue, "CREATED_TIMESTAMP")
        val expected = Pair("CREATED_TIMESTAMP", true)
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeNullReturnedAsEpochWhenCreatedBlank() {
        val nullValue = com.google.gson.JsonNull.INSTANCE
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), nullValue, "")
        val expected = Pair(EPOCH, true)
        assertEquals(expected, actual)
    }

    @Test
    fun testOverwriteFieldValueOverwritesCorrectValue() {
        val id = "OID_WRENCHED_FROM_MONGO_ID"
        val lastModifiedDateTime = "DATETIME_WRENCHED_FROM_MONGO_ID"
        val lastModifiedDateTimeNew = "NEW_DATETIME"
        val obj = com.google.gson.JsonObject()
        obj.addProperty("_id", id)
        obj.addProperty("_lastModifiedDateTime", lastModifiedDateTime)
        obj.addProperty("other", "TEST")
        val expected = com.google.gson.JsonObject()
        expected.addProperty("_id", id)
        expected.addProperty("_lastModifiedDateTime", lastModifiedDateTimeNew)
        expected.addProperty("other", "TEST")
        val actual = hBaseWriter.overwriteFieldValue(Gson(), "_lastModifiedDateTime", lastModifiedDateTimeNew, obj)
        assertEquals(expected, actual)
    }

    @Test
    fun testOptionalDateTimeAsObjectReturnedAsString() {
        val optionalDateTimeField = com.google.gson.JsonObject()
        val optionalDateTimeValue = "DATE_FIELD_VALUE"
        optionalDateTimeField.addProperty("\$date", optionalDateTimeValue)
        val message = com.google.gson.JsonObject()
        val fieldName = "_optionalDateTime"
        message.add(fieldName, optionalDateTimeField)
        val actual = hBaseWriter.optionalDateTime(Gson(), fieldName, message)
        val expected = Pair(optionalDateTimeValue, true)
        assertEquals(expected, actual)
    }

    @Test
    fun testOptionalDateTimeAsStringReturnedAsString() {
        val optionalDateTimeValue = "DATE_FIELD_VALUE"
        val message = com.google.gson.JsonObject()
        val fieldName = "_optionalDateTime"
        message.addProperty(fieldName, optionalDateTimeValue)
        val actual = hBaseWriter.optionalDateTime(Gson(), fieldName, message)
        val expected = Pair(optionalDateTimeValue, false)
        assertEquals(expected, actual)
    }

    @Test
    fun testInvalidOptionalDateTimeAsObjectReturnedAsBlank() {
        val optionalDateTimeField = com.google.gson.JsonObject()
        val optionalDateTimeValue = "DATE_FIELD_VALUE"
        optionalDateTimeField.addProperty("\$invalidProperty", optionalDateTimeValue)
        val message = com.google.gson.JsonObject()
        val fieldName = "_optionalDateTime"
        message.add(fieldName, optionalDateTimeField)
        val actual = hBaseWriter.optionalDateTime(Gson(), fieldName, message)
        val expected = Pair("", true)
        assertEquals(expected, actual)
    }

    @Test
    fun testAbsentOptionalDateTimeAsObjectReturnedAsBlank() {
        val message = com.google.gson.JsonObject()
        val fieldName = "_optionalDateTime"
        message.addProperty("otherProperty", "123")
        val actual = hBaseWriter.optionalDateTime(Gson(), fieldName, message)
        val expected = Pair("", false)
        assertEquals(expected, actual)
    }

    @Test
    fun testPutBatchRetries() {
        try {
            given(hbaseClient.putBatch(any(), any())).willThrow(java.lang.RuntimeException("Failed to put batch"))
            val record = HBaseRecord("key".toByteArray(), "body".toByteArray(), 1.toLong())
            hBaseWriter.putBatch("ns:table", listOf(record))
        }
        catch (e: Exception) {
            verify(hbaseClient, times(5)).putBatch(any(), any())
        }
    }

    @Test
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
            "database.collection.0001.json.gz.enc", mock(), "AAAAAAAAAAAAAAAAAAAAAA=="))

        given(messageUtils.parseGson(any())).willReturn(Gson().fromJson(json, com.google.gson.JsonObject::class.java))
        whenever(keyService.batchDataKey()).thenReturn(DataKeyResult("", "", ""))
        given(cipherService.encrypt(any(), any())).willReturn(EncryptionResult("AAAAAAAAAAAAAAAAAAAAAA==", "qwertyuiop"))
        given(messageProducer.produceMessage(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).willReturn("""{ "message": $json """)
        given(messageUtils.getTimestampAsLong(any())).willReturn(315532800000)
        given(messageUtils.parseJson(any())).willReturn(JsonObject(mapOf(Pair("key", "value"))))
        given(messageUtils.generateKeyFromRecordBody(any())).willReturn("FORMATTED_KEY".toByteArray())
        doNothing().whenever(hBaseWriter).ensureTable(any())
        hBaseWriter.write(items)
        verify(hBaseWriter, times(100)).putBatch(any(), any())
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


