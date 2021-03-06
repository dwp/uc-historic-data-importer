package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.domain.HBaseRecord
import app.services.CipherService
import app.services.FilterService
import app.services.KeyService
import app.services.S3Service
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.beust.klaxon.JsonObject
import com.google.gson.Gson
import com.google.gson.JsonPrimitive
import com.nhaarman.mockitokotlin2.*
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import org.apache.hadoop.hbase.client.Table
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
import org.springframework.test.util.ReflectionTestUtils
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.security.Key
import java.text.ParseException
import java.text.SimpleDateFormat

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HBaseWriter::class])
@TestPropertySource(properties = [
    "s3.bucket=bucket",
    "hbase.retry.max.attempts=5",
    "hbase.retry.initial.backoff=1",
    "hbase.retry.backoff.multiplier=1",
    "max.batch.size.bytes=100",
    "run-mode=import_and_manifest"
])
class HbaseWriterTest {

    val validJsonWithoutId = """{"type":"addressDeclaration"}"""
    val invalidJson2 = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"""
    val validJson = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"}"""
    val validFileName = "adb.collection.0001.json.gz.enc"

    @MockBean
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var filterService: FilterService

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
    private lateinit var s3Service: S3Service

    @SpyBean
    private lateinit var hBaseWriter: HBaseWriter


    @Test
    fun shouldUpdateObjectPriorToEncryption() {
        val date = "\$date"
        val validJson = """{
                |    "_id": {
                |        "declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef",
                |        "createdDateTime": {
                |           "$date": "2000-01-01T00:00:00.000Z" 
                |        }
                |    },
                |    "nullfield": null,
                |    "type":"addressDeclaration",
                |    "_lastModifiedDateTime": {
                |           "$date": "2010-01-01T00:00:00.000Z" 
                |    }
                |}""".trimMargin()

        val json = Gson().fromJson(validJson, com.google.gson.JsonObject::class.java)
        val dumpLine = json.toString()
        val dataKeyResult = DataKeyResult("", "", "")
        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)
        val encryptionResult = EncryptionResult("", "")

        whenever(cipherService.encrypt(any(), any())).thenReturn(encryptionResult)

        val jsonObject = JsonObject()
        whenever(messageUtils.parseGson(dumpLine)).thenReturn(json)
        whenever(messageUtils.parseJson(dumpLine)).thenReturn(jsonObject)
        whenever(messageUtils.getId(jsonObject)).thenReturn(jsonObject)

        whenever(messageUtils.getVersion(any(), any(), any(), any())).thenReturn("100")
        whenever(messageUtils.getTimestampAsLong(any())).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(com.google.gson.JsonObject(), """{ "key": "value" }""",
                false, false, """{ "key": "value" }""", "_lastModifiedDateTime", false, false, false, false, false, encryptionResult, dataKeyResult, "adb",
                "collection")).thenReturn(message)

        val formattedKey = "0000-0000-00001"
        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        doNothing().whenever(hBaseWriter).ensureTable("adb:collection")
        doNothing().whenever(hBaseWriter).putBatch(any(), any())

        val data = listOf(dumpLine)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val dataKeyCaptor = argumentCaptor<String>()
        val lineCaptor = argumentCaptor<String>()
        verify(hBaseWriter, times(1)).encryptDbObject(dataKeyCaptor.capture(), lineCaptor.capture())
        val expectedArgumentJson = """{
                |    "type":"addressDeclaration",
                |    "_id": {
                |        "declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef",
                |        "createdDateTime": "2000-01-01T00:00:00.000+0000" 
                |    },
                |    "nullfield": null,
                |    "_lastModifiedDateTime": "2010-01-01T00:00:00.000+0000"
                |}""".trimMargin()

        assertEquals(Gson().fromJson(expectedArgumentJson, com.google.gson.JsonObject::class.java),
                Gson().fromJson(lineCaptor.firstValue, com.google.gson.JsonObject::class.java))
    }

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

        whenever(messageUtils.getVersion(any(), any(), any(), any())).thenReturn("100")
        whenever(messageUtils.getTimestampAsLong(any())).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(com.google.gson.JsonObject(), """{ "key": "value" }""", 
                false, false, """{ "key": "value" }""", "_lastModifiedDateTime", false, false, false, false, false, encryptionResult, dataKeyResult, "adb",
                "collection")).thenReturn(message)

        val formattedKey = "0000-0000-00001"
        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        doNothing().whenever(hBaseWriter).ensureTable("adb:collection")
        doNothing().whenever(hBaseWriter).putBatch(any(), any())

        val data = listOf(invalidJson2, validJson)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(6)).doAppend(captor.capture())
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
        whenever(messageUtils.getVersion(any(), any(), any(), any())).thenReturn("100")
        whenever(messageUtils.getTimestampAsLong(any())).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(com.google.gson.JsonObject(), """{"key": "value"}""", 
                false, false, """{ "key": "value" }""", "_lastModifiedDateTime", false, false, false, false, false, encryptionResult, dataKeyResult, "adb",
                "collection")).thenReturn(message)
        val formattedKey = "0000-0000-00001"

        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())

        doNothing().whenever(hBaseWriter).ensureTable("adb:collection")
        doNothing().whenever(hBaseWriter).putBatch(any(), any())

        val data = listOf(invalidJson2, validJsonWithoutId)
        val inputStreams = mutableListOf(getInputStream(data, validFileName))
        hBaseWriter.write(inputStreams)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(6)).doAppend(captor.capture())
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

        given(s3Service.objectInputStream("bucket", validFileName)).willReturn(s3InputStream)
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
        val (actualId, actualModified) = hBaseWriter.normalisedId(Gson(), id)
        assertEquals(expectedId, actualId)
        assertEquals(actualModified, HBaseWriter.Companion.IdModification.UnmodifiedObjectId)
    }

    @Test
    fun testIdObjectWithInnerCreatedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat() {
        testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(HBaseWriter.CREATED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerModifiedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat() {
        testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(HBaseWriter.LAST_MODIFIED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerRemovedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat() {
        testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(HBaseWriter.REMOVED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerArchivedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat() {
        testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(HBaseWriter.ARCHIVED_DATE_TIME_FIELD)
    }

    fun testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(dateField: String) {
        val dateInnerField = "\$date"

        val id = """
            {
                "id": "ID",
                "$dateField": {
                    $dateInnerField: "2019-08-05T02:10:19.887+0000"
                }
            }
        """.trimIndent()

        val originalId = Gson().fromJson(id, com.google.gson.JsonObject::class.java)
        val copyOfOriginalId = originalId.deepCopy()
        val (actualId, actualModified) =
                hBaseWriter.normalisedId(Gson(), originalId)

        val expectedId = """
            {
                "id": "ID",
                "$dateField": "2019-08-05T02:10:19.887+0000"
            }
        """.trimIndent()

        assertEquals(Gson().fromJson(expectedId, com.google.gson.JsonObject::class.java).toString(), actualId)
        assertEquals(actualModified, HBaseWriter.Companion.IdModification.FlattenedInnerDate)
        assertEquals(copyOfOriginalId, originalId)
    }

    @Test
    fun testIdObjectWithInnerCreatedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat() {
        testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(HBaseWriter.CREATED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerModifiedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat() {
        testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(HBaseWriter.LAST_MODIFIED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerRemovedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat() {
        testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(HBaseWriter.REMOVED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerArchivedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat() {
        testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(HBaseWriter.ARCHIVED_DATE_TIME_FIELD)
    }

    fun testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(dateField: String) {
        val innerDateField = "\$date"

        val id = """
            {
                "id": "ID",
                "$dateField": {
                    $innerDateField: "2019-08-05T02:10:19.887Z"
                }
            }
        """.trimIndent()

        val (actualId, actualModified) =
                hBaseWriter.normalisedId(Gson(), Gson().fromJson(id, com.google.gson.JsonObject::class.java))

        val expectedId = """
            {
                "id": "ID",
                "$dateField": "2019-08-05T02:10:19.887+0000"
            }
        """.trimIndent()

        assertEquals(Gson().fromJson(expectedId, com.google.gson.JsonObject::class.java).toString(), actualId)
        assertEquals(actualModified, HBaseWriter.Companion.IdModification.FlattenedInnerDate)
    }


    @Test
    fun testIdObjectWithInnerCreatedDateStringReturnedUnchanged() {
        testIdObjectWithInnerDateStringReturnedUnchanged(HBaseWriter.CREATED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerModifiedDateStringReturnedUnchanged() {
        testIdObjectWithInnerDateStringReturnedUnchanged(HBaseWriter.LAST_MODIFIED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerRemovedDateStringReturnedUnchanged() {
        testIdObjectWithInnerDateStringReturnedUnchanged(HBaseWriter.REMOVED_DATE_TIME_FIELD)
    }

    @Test
    fun testIdObjectWithInnerArchivedDateStringReturnedUnchanged() {
        testIdObjectWithInnerDateStringReturnedUnchanged(HBaseWriter.ARCHIVED_DATE_TIME_FIELD)
    }

    fun testIdObjectWithInnerDateStringReturnedUnchanged(dateField: String) {
        val id = """
            {
                "id": "ID",
                "$dateField": "EMBEDDED_DATE_FIELD"
            }
        """.trimIndent()

        val (actualId, actualModified) =
                hBaseWriter.normalisedId(Gson(), Gson().fromJson(id, com.google.gson.JsonObject::class.java))

        assertEquals(Gson().fromJson(id, com.google.gson.JsonObject::class.java).toString(), actualId)
        assertEquals(actualModified, HBaseWriter.Companion.IdModification.UnmodifiedObjectId)
    }

    @Test
    fun testIdObjectWithMultipleInnerDatesInDumpFormatReturnedAsObjectWithFlattenedInnerDatesInKafkaFormat() {
        val innerDateField = "\$date"

        val id = """
            {
                "id": "ID",
                "${HBaseWriter.ARCHIVED_DATE_TIME_FIELD}": {
                    $innerDateField: "2011-08-05T02:10:19.887Z"
                },
                "${HBaseWriter.CREATED_DATE_TIME_FIELD}": {
                    $innerDateField: "2012-08-05T02:10:19.887Z"
                },
                "${HBaseWriter.LAST_MODIFIED_DATE_TIME_FIELD}": {
                    $innerDateField: "2013-08-05T02:10:19.887Z"
                },
                "${HBaseWriter.REMOVED_DATE_TIME_FIELD}": {
                    $innerDateField: "2014-08-05T02:10:19.887Z"
                }
            }
        """.trimIndent()

        val (actualId, actualModified) =
                hBaseWriter.normalisedId(Gson(), Gson().fromJson(id, com.google.gson.JsonObject::class.java))

        val expectedId = """
            {
                "id": "ID",
                "${HBaseWriter.CREATED_DATE_TIME_FIELD}": "2012-08-05T02:10:19.887+0000",
                "${HBaseWriter.LAST_MODIFIED_DATE_TIME_FIELD}": "2013-08-05T02:10:19.887+0000",
                "${HBaseWriter.REMOVED_DATE_TIME_FIELD}": "2014-08-05T02:10:19.887+0000",
                "${HBaseWriter.ARCHIVED_DATE_TIME_FIELD}": "2011-08-05T02:10:19.887+0000"
            }
        """.trimIndent()

        assertEquals(Gson().fromJson(expectedId, com.google.gson.JsonObject::class.java).toString(), actualId)
        assertEquals(actualModified, HBaseWriter.Companion.IdModification.FlattenedInnerDate)
    }

    @Test
    fun testHasDateFieldOnPresenceOfMongoStyleDateField() {
        val dateField = "\$date"
        val id = Gson().fromJson("""
            {
                "id": "ID",
                "createdDateTime": {
                    $dateField: "2019-08-05T02:10:19.887+0000"
                }
            }
        """.trimIndent(), com.google.gson.JsonObject::class.java)
        assertTrue(hBaseWriter.hasDateField(id, "createdDateTime"))
    }

    @Test
    fun Should_Parse_Valid_Incoming_Date_Format() {
        val dateOne = "2019-12-14T15:01:02.000+0000"
        val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
        val expected = df.parse(dateOne)
        val actual = hBaseWriter.getValidParsedDateTime(dateOne)

        assertEquals(expected, actual)
    }

    @Test
    fun Should_Parse_Valid_Outgoing_Date_Format() {
        val dateOne = "2019-12-14T15:01:02.000Z"
        val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        val expected = df.parse(dateOne)
        val actual = hBaseWriter.getValidParsedDateTime(dateOne)

        assertEquals(expected, actual)
    }

    @Test
    fun shouldAcceptNonUtcPositiveOffsetTimeAndReturnAsUtc() {
        val date = "2020-05-12T10:01:02.000+0100"
        val actual = hBaseWriter.kafkaDateFormat(date)
        val expected = "2020-05-12T09:01:02.000+0000"
        assertEquals(expected, actual)
    }

    @Test
    fun shouldAcceptNonUtcNegativeOffsetTimeAndReturnAsUtc() {
        val date = "2020-05-12T10:01:02.000-0100"
        val actual = hBaseWriter.kafkaDateFormat(date)
        val expected = "2020-05-12T11:01:02.000+0000"
        assertEquals(expected, actual)
    }

    @Test
    fun shouldAcceptUtcTimeAndReturnAsUtc() {
        val date = "2020-05-12T10:01:02.000+0000"
        val actual = hBaseWriter.kafkaDateFormat(date)
        val expected = "2020-05-12T10:01:02.000+0000"
        assertEquals(expected, actual)
    }

    @Test
    fun shouldAcceptNonUtcTimeAndReturnAsUtc() {
        val date = "2020-05-12T10:01:02.000+0100"
        val actual = hBaseWriter.kafkaDateFormat(date)
        val expected = "2020-05-12T09:01:02.000+0000"
        assertEquals(expected, actual)
    }

    @Test
    fun Should_Throw_Error_With_Invalid_Date_Format() {
        val exception = shouldThrow<Exception> {
            hBaseWriter.getValidParsedDateTime("2019-12-14T15:01:02")
        }

        exception.message shouldBe "Unparseable date found: '2019-12-14T15:01:02', did not match any supported date formats"
    }

    @Test
    fun testHasDateFieldReturnsFalseOnPresenceOfNonMongoStyleDateFieldWithExtraField() {
        val dateField = "\$date"
        val id = Gson().fromJson("""
            {
                "id": "ID",
                "createdDateTime": {
                    $dateField: "2019-08-05T02:10:19.887+0000",
                    "additionalField": "ABC"
                }
            }
        """.trimIndent(), com.google.gson.JsonObject::class.java)
        assertTrue(!hBaseWriter.hasDateField(id, "createdDateTime"))
    }

    @Test
    fun testHasDateFieldReturnsFalseOnPresenceOfNonMongoStyleDateObjectField() {
        val id = Gson().fromJson("""
            {
                "id": "ID",
                "createdDateTime": {
                    "additionalField": "ABC"
                }
            }
        """.trimIndent(), com.google.gson.JsonObject::class.java)
        assertTrue(!hBaseWriter.hasDateField(id, "createdDateTime"))
    }

    @Test
    fun testHasDateFieldReturnsFalseOnPresenceOfStringStyleDateField() {
        val id = Gson().fromJson("""
            {
                "id": "ID",
                "createdDateTime": "2019-08-05T02:10:19.887+0000"
            }
        """.trimIndent(), com.google.gson.JsonObject::class.java)
        assertTrue(!hBaseWriter.hasDateField(id, "createdDateTime"))
    }

    @Test
    fun testHasDateFieldReturnsFalseOnAbsenceOfDateField() {
        val id = Gson().fromJson("""
            {
                "id": "ID"
            }
        """.trimIndent(), com.google.gson.JsonObject::class.java)
        assertTrue(!hBaseWriter.hasDateField(id, "createdDateTime"))
    }

    @Test
    fun testIdStringReturnedAsString() {
        val id = JsonPrimitive("id")
        val actual = hBaseWriter.normalisedId(Gson(), id)
        assertEquals(Pair("id", HBaseWriter.Companion.IdModification.UnmodifiedStringId), actual)
    }

    @Test
    fun testMongoIdStringReturnedAsString() {
        val oid = com.google.gson.JsonObject()
        val oidValue = "OID_VALUE"
        oid.addProperty("\$oid", oidValue)
        val actual = hBaseWriter.normalisedId(Gson(), oid)
        assertEquals(Pair(oidValue, HBaseWriter.Companion.IdModification.FlattenedMongoId), actual)
    }

    @Test
    fun testIdNumberReturnedAsObject() {
        val id = JsonPrimitive( 12345)
        val actual = hBaseWriter.normalisedId(Gson(), id)
        val expectedId = "12345"
        assertEquals(Pair(expectedId, HBaseWriter.Companion.IdModification.UnmodifiedStringId), actual)
    }

    @Test
    fun testIdArrayReturnedAsNull() {
        val arrayValue = com.google.gson.JsonArray()
        arrayValue.add("1")
        arrayValue.add("2")
        val actual = hBaseWriter.normalisedId(Gson(), arrayValue)
        val expected = Pair("", HBaseWriter.Companion.IdModification.InvalidId)
        assertEquals(expected, actual)
    }

    @Test
    fun testIdNullReturnedAsEmpty() {
        val nullValue = com.google.gson.JsonNull.INSTANCE
        val actual = hBaseWriter.normalisedId(Gson(), nullValue)
        val expected = Pair("", HBaseWriter.Companion.IdModification.InvalidId)
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeAsNonDateObjectReturnedAsCreated() {
        val lastModified = com.google.gson.JsonObject()
        val lastModifiedValue = "testDateField"
        lastModified.addProperty("\$notDate", lastModifiedValue)
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), lastModified, "CREATED_TIMESTAMP")
        val expected = Pair("CREATED_TIMESTAMP", "createdDateTime")
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeAsDateObjectInDumpFormatReturnedAsDateFieldValueInKafkaFormat() {
        val lastModified = com.google.gson.JsonObject()
        val lastModifiedValue = "2019-08-05T02:10:19.887Z"
        lastModified.addProperty("\$date", lastModifiedValue)
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), lastModified, "CREATED_TIMESTAMP")
        val expected = Pair("2019-08-05T02:10:19.887+0000", "_lastModifiedDateTimeStripped")
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeAsDateObjectInKafkaFormatReturnedAsDateFieldValueInKafkaFormat() {
        val lastModified = com.google.gson.JsonObject()
        val lastModifiedValue = "2019-08-05T02:10:19.887+0000"
        lastModified.addProperty("\$date", lastModifiedValue)
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), lastModified, "CREATED_TIMESTAMP")
        val expected = Pair("2019-08-05T02:10:19.887+0000", "_lastModifiedDateTimeStripped")
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeAsStringReturnedAsValue() {
        val lastModified = JsonPrimitive("testDateString")
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), lastModified, "CREATED_TIMESTAMP")
        val expected = Pair("testDateString", "_lastModifiedDateTime")
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeArrayReturnedAsCreatedWhenCreatedNotBlank() {
        val arrayValue = com.google.gson.JsonArray()
        arrayValue.add("1")
        arrayValue.add("2")
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), arrayValue, "")
        val expected = Pair(HBaseWriter.EPOCH, "epoch")
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeNullReturnedAsCreatedWhenCreatedNotBlank() {
        val nullValue = com.google.gson.JsonNull.INSTANCE
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), nullValue, "CREATED_TIMESTAMP")
        val expected = Pair("CREATED_TIMESTAMP", "createdDateTime")
        assertEquals(expected, actual)
    }

    @Test
    fun testLastModifiedDateTimeNullReturnedAsEpochWhenCreatedBlank() {
        val nullValue = com.google.gson.JsonNull.INSTANCE
        val actual = hBaseWriter.lastModifiedDateTime(Gson(), nullValue, "")
        val expected = Pair(HBaseWriter.EPOCH, "epoch")
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
    fun testOverwriteFieldWithObjectOverwritesCorrectValue() {
        val id = Gson().fromJson("""{
        |    "key1": "val1",
        |    "key2": "val2"
        |}""".trimMargin(), com.google.gson.JsonObject::class.java)


        val obj = Gson().fromJson("""{
        |    "_id": "OLD_ID",
        |    "other_field": "OTHER_FIELD_VALUE"   
        |}""".trimMargin(), com.google.gson.JsonObject::class.java)

        val actual = hBaseWriter.overwriteFieldValueWithObject(Gson(),
                "_id", id, obj)

        val expected = Gson().fromJson("""{
        |   "_id": {
        |       "key1": "val1",
        |       "key2": "val2"
        |   },
        |   "other_field": "OTHER_FIELD_VALUE"   
        }""".trimMargin(), com.google.gson.JsonObject::class.java)

        assertEquals(expected, actual)
    }

    @Test
    fun testOptionalDateTimeInDumpFormatReturnedAsStringInKafkaFormat() {
        val optionalDateTimeField = com.google.gson.JsonObject()
        val optionalDateTimeValue = "2019-08-05T02:10:19.887Z"
        optionalDateTimeField.addProperty("\$date", optionalDateTimeValue)
        val message = com.google.gson.JsonObject()
        val fieldName = "_optionalDateTime"
        message.add(fieldName, optionalDateTimeField)
        val actual = hBaseWriter.optionalDateTime(Gson(), fieldName, message)
        val expected = Pair("2019-08-05T02:10:19.887+0000", true)
        assertEquals(expected, actual)
    }

    @Test
    fun testOptionalDateTimeInKafkaFormatReturnedAsStringInKafkaFormat() {
        val optionalDateTimeField = com.google.gson.JsonObject()
        val optionalDateTimeValue = "2019-08-05T02:10:19.887+0000"
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
    fun testReformatNonRemovedReturnsUnmodifiedRecordWhenNoRemovedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val recordWithRemovedElement = """{ "_notRemoved": $innerRecord }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        assertEquals(expected, actual)
        assertEquals(wasChanged, false)
    }

    @Test
    fun testReformatRemovedReturnsInnerRecordWhenRemovedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val recordWithRemovedElement = """{ "_removed": $innerRecord }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson(innerRecord, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatRemovedOverwritesTypeAttribute() {
        val innerRecord = """{ "_id": "123456789", "@type": "MONGO_INSERT" }"""
        val recordWithRemovedElement = """{ "_removed": $innerRecord }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson("""{ "_id": "123456789", "@type": "MONGO_DELETE" }""", com.google.gson.JsonObject::class.java)
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatRemovedReturnsOuterRecordWhenRemovedElementDoesNotExist() {
        val outerRecord = """{ "_id": "123456789" }""".trimIndent()
        val json = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(outerRecord)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(outerRecord)
        val expected = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
        assertEquals(expected, actual)
        assertEquals(wasChanged, false)
    }

    @Test
    fun testReformatTransplantsLastModifiedWhenRemovedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
        val recordWithRemovedElement = """{ "_removed": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatReplacesLastModifiedWhenRemovedElementExists() {
        val innerRecord = """{ "_id": "123456789", "_lastModifiedDateTime": "INNER_LAST_MODIFIED" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
        val recordWithRemovedElement = """{ "_removed": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatTransplantsRemovedTimeWhenRemovedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "_removedDateTime": "OUTER_REMOVED_TIME" }""".trimIndent()
        val recordWithRemovedElement = """{ "_removed": $innerRecord, "_removedDateTime": "OUTER_REMOVED_TIME" }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatReplacesRemovedTimeWhenRemovedElementExists() {
        val innerRecord = """{ "_id": "123456789", "_removedDateTime": "INNER_REMOVED_TIME" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "_removedDateTime": "OUTER_REMOVED_TIME" }""".trimIndent()
        val recordWithRemovedElement = """{ "_removed": $innerRecord, "_removedDateTime": "OUTER_REMOVED_TIME" }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatTransplantsTimestampWhenRemovedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
        val recordWithRemovedElement = """{ "_removed": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatReplacesTimestampWhenRemovedElementExists() {
        val innerRecord = """{ "_id": "123456789", "timestamp": "INNER_TIMESTAMP" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
        val recordWithRemovedElement = """{ "_removed": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
        val json = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
        whenever(messageUtils.parseGson(recordWithRemovedElement)).thenReturn(json)
        val (actual, wasChanged) = hBaseWriter.reformatRemoved(recordWithRemovedElement)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatNonArchivedReturnsUnmodifiedRecordWhenNoArchivedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val recordWithArchivedElement = """{ "_notArchived": $innerRecord }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        assertEquals(expected, actual)
        assertEquals(wasChanged, false)
    }

    @Test
    fun testReformatArchivedReturnsInnerRecordWhenArchivedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val recordWithArchivedElement = """{ "_archived": $innerRecord }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(innerRecord, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatArchivedOverwritesTypeAttribute() {
        val innerRecord = """{ "_id": "123456789", "@type": "MONGO_INSERT" }"""
        val recordWithArchivedElement = """{ "_archived": $innerRecord }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson("""{ "_id": "123456789", "@type": "MONGO_DELETE" }""", com.google.gson.JsonObject::class.java)
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatArchivedReturnsOuterRecordWhenArchivedElementDoesNotExist() {
        val outerRecord = """{ "_id": "123456789" }""".trimIndent()
        val json = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
        assertEquals(expected, actual)
        assertEquals(wasChanged, false)
    }

    @Test
    fun testReformatTransplantsLastModifiedWhenArchivedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
        val recordWithArchivedElement = """{ "_archived": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatReplacesLastModifiedWhenArchivedElementExists() {
        val innerRecord = """{ "_id": "123456789", "_lastModifiedDateTime": "INNER_LAST_MODIFIED" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
        val recordWithArchivedElement = """{ "_archived": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatTransplantsTimestampWhenArchivedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
        val recordWithArchivedElement = """{ "_archived": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatReplacesTimestampWhenArchivedElementExists() {
        val innerRecord = """{ "_id": "123456789", "timestamp": "INNER_TIMESTAMP" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
        val recordWithArchivedElement = """{ "_archived": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatTransplantsArchivedTimeWhenArchivedElementExists() {
        val innerRecord = """{ "_id": "123456789" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "_archivedDateTime": "OUTER_ARCHIVED_TIME" }""".trimIndent()
        val recordWithArchivedElement = """{ "_archived": $innerRecord, "_archivedDateTime": "OUTER_ARCHIVED_TIME" }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testReformatReplacesArchivedTimeWhenArchivedElementExists() {
        val innerRecord = """{ "_id": "123456789", "_archivedDateTime": "INNER_ARCHIVED_TIME" }""".trimIndent()
        val innerRecordWithTimestamp = """{ "_id": "123456789", "_archivedDateTime": "OUTER_ARCHIVED_TIME" }""".trimIndent()
        val recordWithArchivedElement = """{ "_archived": $innerRecord, "_archivedDateTime": "OUTER_ARCHIVED_TIME" }"""
        val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
        val (actual, wasChanged) = hBaseWriter.reformatArchived(json)
        val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
        expected.addProperty("@type", "MONGO_DELETE")
        assertEquals(expected, actual)
        assertEquals(wasChanged, true)
    }

    @Test
    fun testCopyWhenFieldExistsInSourceButNotTarget() {
        val sourceRecord = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
        val targetRecord = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
        val expected = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE", "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
        hBaseWriter.copyField("SOURCE_KEY", sourceRecord, targetRecord)
        assertEquals(expected, targetRecord)
    }

    @Test
    fun testCopyWhenFieldExistsInSourceAndTarget() {
        val sourceRecord = Gson().fromJson("""{ "SHARED_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
        val targetRecord = Gson().fromJson("""{ "SHARED_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
        val expected = Gson().fromJson("""{ "SHARED_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
        hBaseWriter.copyField("SHARED_KEY", sourceRecord, targetRecord)
        assertEquals(expected, targetRecord)
    }

    @Test
    fun testCopyWhenFieldNotInSource() {
        val sourceRecord = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
        val targetRecord = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
        val expected = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
        hBaseWriter.copyField("ABSENT_KEY", sourceRecord, targetRecord)
        assertEquals(expected, targetRecord)
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
    fun testCoalesced() {
        val dumplist = setOf("accessCodeThrottle",
                "accessCodeVerification",
                "accessList",
                "accountVerification",
                "actionedPayments",
                "activeAdvanceProcess",
                "additionalClaimantInformation",
                "address",
                "addressDeclaration",
                "advanceDebtPosition",
                "advanceDetails",
                "advanceGroup",
                "advanceProcessInitialisationFailure",
                "agent",
                "agentAction",
                "agentAssignment",
                "agentFeedback",
                "agentGuidance",
                "agentGuidanceTracking",
                "agentPrivileges",
                "agentsInTeam",
                "agentSupportRoles",
                "agentTakenTime",
                "agentThirdPartyReference",
                "agentToDo",
                "agentToDoArchive",
                "agentToDoImport",
                "agentWorkGroupAllocation",
                "alternativePaymentArrangement",
                "apa",
                "apaPayment",
                "appointment",
                "appointmentReminderJobMetadata",
                "appointmentTypesByJobcentre",
                "archiveRecord",
                "assessmentPeriod",
                "attachmentMetaData",
                "attachments",
                "attachmentsData",
                "attachmentsMetadata",
                "authentication",
                "authenticationRequest",
                "availability",
                "awardModification",
                "bankAccountTransactionData",
                "bankDetails",
                "bankDetailsDeclaration",
                "blockedTime",
                "blockedTimeSlot",
                "bookedEvidenceOrPsnAppointment",
                "bulkRentChangeRequest",
                "calculatedDeductions",
                "calculateDeductions",
                "calculation",
                "calculationParts-archived",
                "calculationParts-eight",
                "calculationParts-eighteen",
                "calculationParts-eleven",
                "calculationParts-fifteen",
                "calculationParts-five",
                "calculationParts-four",
                "calculationParts-fourteen",
                "calculationParts-nine",
                "calculationParts-nineteen",
                "calculationParts-one",
                "calculationParts-seven",
                "calculationParts-seventeen",
                "calculationParts-six",
                "calculationParts-sixteen",
                "calculationParts-ten",
                "calculationParts-thirteen",
                "calculationParts-thirty",
                "calculationParts-thirtyone",
                "calculationParts-thirtytwo",
                "calculationParts-three",
                "calculationParts-twelve",
                "calculationParts-twenty",
                "calculationParts-twentyeight",
                "calculationParts-twentyfive",
                "calculationParts-twentyfour",
                "calculationParts-twentynine",
                "calculationParts-twentyone",
                "calculationParts-twentyseven",
                "calculationParts-twentysix",
                "calculationParts-twentythree",
                "calculationParts-twentytwo",
                "calculationParts-two",
                "calculatorClaim",
                "capital",
                "carerCircumstances",
                "carerDeclaration",
                "carersAllowance",
                "carerTimeOff",
                "caringDeclaration",
                "caseLoadEvent",
                "checkAutoCalcPendingData",
                "childcare",
                "childcareDeclaration",
                "childcareTransactionData",
                "childDeclaration",
                "childElementEligibility",
                "childrenCircumstances",
                "childrenVerifiedCircumstancesConversation",
                "claimant",
                "claimantAccessType",
                "claimantAttachmentUpload",
                "claimantCommitment",
                "claimantCorporateAppointee",
                "claimantEligibilityDetails",
                "claimantEvent",
                "claimantHistory",
                "claimantIdentity",
                "claimantIdentityVerification",
                "claimantLanguagePreference",
                "claimantNotification",
                "claimantProfile",
                "claimantSpareRoomSubsidyEligibility",
                "claimantStats",
                "claimantUploadDetails",
                "claimantVerifyIdentity",
                "claimHistoryEntry-archived",
                "claimHistoryEntry-eight",
                "claimHistoryEntry-eighteen",
                "claimHistoryEntry-eleven",
                "claimHistoryEntry-fifteen",
                "claimHistoryEntry-five",
                "claimHistoryEntry-four",
                "claimHistoryEntry-fourteen",
                "claimHistoryEntry-nine",
                "claimHistoryEntry-nineteen",
                "claimHistoryEntry-one",
                "claimHistoryEntry-seven",
                "claimHistoryEntry-seventeen",
                "claimHistoryEntry-six",
                "claimHistoryEntry-sixteen",
                "claimHistoryEntry-ten",
                "claimHistoryEntry-thirteen",
                "claimHistoryEntry-thirty",
                "claimHistoryEntry-thirtyone",
                "claimHistoryEntry-thirtytwo",
                "claimHistoryEntry-three",
                "claimHistoryEntry-twelve",
                "claimHistoryEntry-twenty",
                "claimHistoryEntry-twentyeight",
                "claimHistoryEntry-twentyfive",
                "claimHistoryEntry-twentyfour",
                "claimHistoryEntry-twentynine",
                "claimHistoryEntry-twentyone",
                "claimHistoryEntry-twentyseven",
                "claimHistoryEntry-twentysix",
                "claimHistoryEntry-twentythree",
                "claimHistoryEntry-twentytwo",
                "claimHistoryEntry-two",
                "coldWeatherPayment",
                "configuration",
                "confirmBenefitsDetailsToDoTimestamp",
                "contract",
                "contractEvent",
                "contractStats",
                "controlMeasure",
                "conversion-status",
                "corporateAppointeeBody",
                "corporateAppointeeBodyAddress",
                "corporateAppointeeBodyBankDetails",
                "corporateAppointeesTfaSmsTransactionMetadata",
                "corporateAppointeeUserAccount",
                "covid19ClaimSelectorLog",
                "covid19DeductionsBypassLog",
                "covid19PrepaymentBypassLog",
                "covidAffected",
                "covidAffectedToDo",
                "cpsPaymentFileRecord",
                "cyiClaimantRequest",
                "cyiLoginToken",
                "debtControlSheet",
                "debtControlSheetToDoData",
                "debtInterest",
                "debtPosition",
                "declaration",
                "declarationOverrideInterval",
                "deductionsStatus",
                "deductionsStatusRequired",
                "deferredConsiderClosingClaimDecision",
                "delayedMessage",
                "deliveryUnitAddress",
                "deliveryUnitThirdPartyReference",
                "depsAndNonDepsSpareRoomSubsidyEligibility",
                "destinationStatistics",
                "digimanData",
                "digimanDataArchive",
                "disclosure-archived",
                "disclosureData-archived",
                "disclosureData-eight",
                "disclosureData-eighteen",
                "disclosureData-eleven",
                "disclosureData-fifteen",
                "disclosureData-five",
                "disclosureData-four",
                "disclosureData-fourteen",
                "disclosureData-nine",
                "disclosureData-nineteen",
                "disclosureData-one",
                "disclosureData-seven",
                "disclosureData-seventeen",
                "disclosureData-six",
                "disclosureData-sixteen",
                "disclosureData-ten",
                "disclosureData-thirteen",
                "disclosureData-thirty",
                "disclosureData-thirtyone",
                "disclosureData-thirtytwo",
                "disclosureData-three",
                "disclosureData-twelve",
                "disclosureData-twenty",
                "disclosureData-twentyeight",
                "disclosureData-twentyfive",
                "disclosureData-twentyfour",
                "disclosureData-twentynine",
                "disclosureData-twentyone",
                "disclosureData-twentyseven",
                "disclosureData-twentysix",
                "disclosureData-twentythree",
                "disclosureData-twentytwo",
                "disclosureData-two",
                "disclosure-eight",
                "disclosure-eighteen",
                "disclosure-eleven",
                "disclosure-fifteen",
                "disclosure-five",
                "disclosure-four",
                "disclosure-fourteen",
                "disclosure-nine",
                "disclosure-nineteen",
                "disclosure-one",
                "disclosure-seven",
                "disclosure-seventeen",
                "disclosure-six",
                "disclosure-sixteen",
                "disclosure-ten",
                "disclosure-thirteen",
                "disclosure-thirty",
                "disclosure-thirtyone",
                "disclosure-thirtytwo",
                "disclosure-three",
                "disclosure-twelve",
                "disclosure-twenty",
                "disclosure-twentyeight",
                "disclosure-twentyfive",
                "disclosure-twentyfour",
                "disclosure-twentynine",
                "disclosure-twentyone",
                "disclosure-twentyseven",
                "disclosure-twentysix",
                "disclosure-twentythree",
                "disclosure-twentytwo",
                "disclosure-two",
                "earlyEstimatedCalculationParts",
                "earlyStatement",
                "earlyStatementContractId",
                "earningsData",
                "educationCircumstances",
                "educationDeclaration",
                "eligibilityCheckResponse",
                "emailSearch",
                "emailToClaimantMap",
                "emailVerificationToken",
                "employmentCircumstances",
                "encryptedData",
                "encryptedFileMetaData",
                "endOfAssessmentPeriodProcessStatus",
                "entitlementReason",
                "estimatedCalculation",
                "estimatedCalculationArchive",
                "estimatedCalculationParts-archived",
                "estimatedCalculationParts-eight",
                "estimatedCalculationParts-eighteen",
                "estimatedCalculationParts-eleven",
                "estimatedCalculationParts-fifteen",
                "estimatedCalculationParts-five",
                "estimatedCalculationParts-four",
                "estimatedCalculationParts-fourteen",
                "estimatedCalculationParts-nine",
                "estimatedCalculationParts-nineteen",
                "estimatedCalculationParts-one",
                "estimatedCalculationParts-seven",
                "estimatedCalculationParts-seventeen",
                "estimatedCalculationParts-six",
                "estimatedCalculationParts-sixteen",
                "estimatedCalculationParts-ten",
                "estimatedCalculationParts-thirteen",
                "estimatedCalculationParts-thirty",
                "estimatedCalculationParts-thirtyone",
                "estimatedCalculationParts-thirtytwo",
                "estimatedCalculationParts-three",
                "estimatedCalculationParts-twelve",
                "estimatedCalculationParts-twenty",
                "estimatedCalculationParts-twentyeight",
                "estimatedCalculationParts-twentyfive",
                "estimatedCalculationParts-twentyfour",
                "estimatedCalculationParts-twentynine",
                "estimatedCalculationParts-twentyone",
                "estimatedCalculationParts-twentyseven",
                "estimatedCalculationParts-twentysix",
                "estimatedCalculationParts-twentythree",
                "estimatedCalculationParts-twentytwo",
                "estimatedCalculationParts-two",
                "estimatedDeductions",
                "expectedEvidenceDocuments",
                "extractJob",
                "extractSeqNo",
                "failedKafkaMessages",
                "failedMessage",
                "failedMigrationEncryptedData",
                "fasterPaymentStatus",
                "featureConfiguration",
                "featureToggles",
                "financialHardshipStatus",
                "firstCommitmentAppointmentsNotRequired",
                "fraudPenalty",
                "fraudPenaltyProgress",
                "gender",
                "genie",
                "gracePeriod",
                "guardiansAllowance",
                "healthAndDisabilityCircumstances",
                "healthAndDisabilityDeclaration",
                "healthCircumstances",
                "healthDeclaration",
                "historicRteData",
                "historicRteEarnings",
                "hmrcChildDataProvision",
                "holdingEncryptedData",
                "holdingMigrated",
                "housingCircumstances",
                "housingDeclaration",
                "housingEntitlement",
                "hrtAppointmentRequired",
                "idempotentNotificationMongoDocument",
                "identityVerificationDetails",
                "ineligiblePartner",
                "insolvencyOrder",
                "interaction",
                "interestPerson",
                "invalidUsernameIdentity",
                "ipAddressConfidence",
                "job",
                "jobStatus",
                "jounal",
                "journal-archived",
                "journal-eight",
                "journal-eighteen",
                "journal-eleven",
                "journal-fifteen",
                "journal-five",
                "journal-four",
                "journal-fourteen",
                "journal-nine",
                "journal-nineteen",
                "journal-one",
                "journal-seven",
                "journal-seventeen",
                "journal-six",
                "journal-sixteen",
                "journal-ten",
                "journal-thirteen",
                "journal-thirty",
                "journal-thirtyone",
                "journal-thirtytwo",
                "journal-three",
                "journal-twelve",
                "journal-twenty",
                "journal-twentyeight",
                "journal-twentyfive",
                "journal-twentyfour",
                "journal-twentynine",
                "journal-twentyone",
                "journal-twentyseven",
                "journal-twentysix",
                "journal-twentythree",
                "journal-twentytwo",
                "journal-two",
                "ladsNotificationDetail",
                "landlordAddress",
                "landlordPortalTenancy",
                "landlordToDo",
                "landlordToDoLock",
                "languagePreference",
                "languagePreferenceDeclaration",
                "lastLogin",
                "lastSavedPrepareCommitments",
                "localUserMatchingData",
                "login",
                "managementInformation",
                "manualAdvancePayment",
                "manualAwardPayment",
                "manualDeductionsCalculationData",
                "manualOverride",
                "matchingServiceRequests",
                "moveToUc",
                "moveToUcClaim",
                "moveToUcRequest",
                "nationalCostsRuleReferenceData",
                "nationality",
                "nationalityDeclaration",
                "ninoCheck",
                "nonLandlordApaRequest",
                "nonStandardHousingAssessment",
                "notificationSettings",
                "notifyCisCache",
                "notifyThirdPartyDeductionToDoData",
                "onGoingInterest",
                "onlyOnceMongoDocument",
                "organisation",
                "otherBenefit",
                "otherBenefitAgentDeclaration",
                "otherBenefitRelatedAction",
                "otherBenefitsNotificationTimestamp",
                "otherIncome",
                "otherIncomeDeclaration",
                "outOfClaimDays",
                "overlappingBenefit",
                "overpayment",
                "partialDisclosure",
                "partnerQuestion",
                "partnerQuestionDeclaration",
                "payment",
                "paymentIC114827Backup",
                "paymentKeys",
                "paymentReference",
                "pendingAdvanceData",
                "pendingAuthorisationCache",
                "pendingNotification",
                "pendingStatement",
                "periodOfSickness",
                "persistedErrors",
                "personalDetails",
                "personDetails",
                "pinChallenge",
                "pinnedNote",
                "pregnancy",
                "pregnancyDeclaration",
                "prepaymentCheckFailure",
                "previousEarnings",
                "properties",
                "propertyAddress",
                "propertyAddressListUpdate",
                "proveYourIdentityTrial",
                "provideTenancyDetailsRequest",
                "qflowEnabledJobcentre",
                "quartz",
                "quartz_calendars",
                "quartz_jobs",
                "quartz_locks",
                "quartz_paused_trigger_groups",
                "quartz_schedulers",
                "quartz_triggers",
                "recalculateAgentStats",
                "recoverableHardshipPayment",
                "registrationTokenAllocation",
                "relationshipDeclaration",
                "reportSelfEmploymentEarningsFeedback",
                "requiredAuthorisationEscalation",
                "resolvedProperties-archived",
                "resolvedProperties-eight",
                "resolvedProperties-eighteen",
                "resolvedProperties-eleven",
                "resolvedProperties-fifteen",
                "resolvedProperties-five",
                "resolvedProperties-four",
                "resolvedProperties-fourteen",
                "resolvedProperties-nine",
                "resolvedProperties-nineteen",
                "resolvedProperties-one",
                "resolvedProperties-seven",
                "resolvedProperties-seventeen",
                "resolvedProperties-six",
                "resolvedProperties-sixteen",
                "resolvedProperties-ten",
                "resolvedProperties-thirteen",
                "resolvedProperties-thirty",
                "resolvedProperties-thirtyone",
                "resolvedProperties-thirtytwo",
                "resolvedProperties-three",
                "resolvedProperties-twelve",
                "resolvedProperties-twenty",
                "resolvedProperties-twentyeight",
                "resolvedProperties-twentyfive",
                "resolvedProperties-twentyfour",
                "resolvedProperties-twentynine",
                "resolvedProperties-twentyone",
                "resolvedProperties-twentyseven",
                "resolvedProperties-twentysix",
                "resolvedProperties-twentythree",
                "resolvedProperties-twentytwo",
                "resolvedProperties-two",
                "rolePrivileges",
                "rteInterestSet",
                "rtiFeed",
                "saltData",
                "sanction",
                "sanctionEscalation",
                "sanctionProgress",
                "sanctionTerminationDecision",
                "savingsAndInvestmentDeclaration",
                "selfEmploymentLoss",
                "sentAppointmentReminder",
                "session",
                "sessionTraceable",
                "srsLandlordBankDetails",
                "srsRentChanges",
                "stagedNotification",
                "statement",
                "statementAndComparison",
                "stopTaxCreditNotice",
                "storedPassword",
                "summary",
                "supportForMortgageInterest",
                "supportMortgageInterestEligibilityDetails",
                "supportMortgageInterestLoanAccepted",
                "survey",
                "suspiciousCalculation",
                "systemWorkGroupAllocation",
                "team",
                "teamMemberStats",
                "telephonyClaimApplication",
                "telephonyClaimApplicationTransition",
                "temp",
                "temporaryAccess",
                "tenancyAgreement",
                "terminalIllness",
                "testCollection",
                "tfaDisabledLandlord",
                "tfaLastSuccessfulAttempt",
                "tfaSmsUser",
                "thirdParty",
                "thirdPartyAuthenticationTransactionMetadata",
                "thirdPartyCreditor",
                "thirdPartyDeduction",
                "thirdPartyDeductionPriority",
                "thirdPartyLandlordTransactionMetadata",
                "thirdPartyUsersTransactionMetadata",
                "threatLevel",
                "toDo",
                "traceable",
                "trackedNotification",
                "transactionResults",
                "transitionalProtection",
                "trialGroupParameters",
                "trialParticipant",
                "triggerHistory",
                "untrustedIdentity",
                "UpdateMongoLock_acceptedDataService",
                "UpdateMongoLock_advances",
                "UpdateMongoLock_agentAccessService",
                "UpdateMongoLock_agentAttachment",
                "UpdateMongoLock_agentCore",
                "UpdateMongoLock_agentSession",
                "UpdateMongoLock_appointments",
                "UpdateMongoLock_authenticator",
                "UpdateMongoLock_calculationProcess",
                "UpdateMongoLock_calculator",
                "UpdateMongoLock_childrenCore",
                "UpdateMongoLock_claimantAdvances",
                "UpdateMongoLock_claimantCommunicationService",
                "UpdateMongoLock_claimantHistory",
                "UpdateMongoLock_claimantSession",
                "UpdateMongoLock_claimantTfaSms",
                "UpdateMongoLock_coldWeatherPaymentsCore",
                "UpdateMongoLock_core",
                "UpdateMongoLock_corporateAppointeesCore",
                "UpdateMongoLock_corporateAppointeesTfaSms",
                "UpdateMongoLock_cryptoCore",
                "UpdateMongoLock_cryptoOps",
                "UpdateMongoLock_dataAcceptanceService",
                "UpdateMongoLock_dataArchivingCore",
                "UpdateMongoLock_deductions",
                "UpdateMongoLock_deductionsProcess",
                "UpdateMongoLock_hrtEntitlement",
                "UpdateMongoLock_identity",
                "UpdateMongoLock_laidPostcodeLookupCore",
                "UpdateMongoLock_landlordTfaSms",
                "UpdateMongoLock_matchingService",
                "UpdateMongoLock_moveToUcCore",
                "UpdateMongoLock_overpayments",
                "UpdateMongoLock_paymentsCore",
                "UpdateMongoLock_paymentsProcess",
                "UpdateMongoLock_penaltiesAndDeductions",
                "UpdateMongoLock_personalDetails",
                "UpdateMongoLock_propertyAddressLookupCore",
                "UpdateMongoLock_quartz",
                "UpdateMongoLock_riskDataService",
                "UpdateMongoLock_selfEmploymentClaimantCore",
                "UpdateMongoLock_sessionConfidence",
                "UpdateMongoLock_supportCore",
                "UpdateMongoLock_systemCore",
                "UpdateMongoLock_teamCore",
                "UpdateMongoLock_thirdParties",
                "UpdateMongoLock_thirdPartiesIntegration",
                "UpdateMongoLock_thirdPartyAuthentication",
                "UpdateMongoLock_thirdPartyLandlord",
                "UpdateMongoLock_thirdPartySession",
                "UpdateMongoLock_thirdPartyUsers",
                "UpdateMongoLock_token",
                "UpdateMongoLock_workCapabilityAssessment",
                "UpdateMongoLock_workSearchCore",
                "urlVisit",
                "userAssociation",
                "verifyIdentityAssessment",
                "watchList",
                "watchListMatches",
                "wizard",
                "workAndEarningsCircumstances",
                "workAndEarningsDeclaration",
                "workCapabilityAssessmentDecision",
                "workCapabilityAssessmentDeclaration",
                "workGroupAllocation",
                "youthObligationDetails")

        val expected = setOf("accessCodeThrottle",
                "accessCodeVerification",
                "accessList",
                "accountVerification",
                "actionedPayments",
                "activeAdvanceProcess",
                "additionalClaimantInformation",
                "address",
                "addressDeclaration",
                "advanceDebtPosition",
                "advanceDetails",
                "advanceGroup",
                "advanceProcessInitialisationFailure",
                "agent",
                "agentAction",
                "agentAssignment",
                "agentFeedback",
                "agentGuidance",
                "agentGuidanceTracking",
                "agentPrivileges",
                "agentsInTeam",
                "agentSupportRoles",
                "agentTakenTime",
                "agentThirdPartyReference",
                "agentToDo",
                "agentToDoArchive",
                "agentToDoImport",
                "agentWorkGroupAllocation",
                "alternativePaymentArrangement",
                "apa",
                "apaPayment",
                "appointment",
                "appointmentReminderJobMetadata",
                "appointmentTypesByJobcentre",
                "archiveRecord",
                "assessmentPeriod",
                "attachmentMetaData",
                "attachments",
                "attachmentsData",
                "attachmentsMetadata",
                "authentication",
                "authenticationRequest",
                "availability",
                "awardModification",
                "bankAccountTransactionData",
                "bankDetails",
                "bankDetailsDeclaration",
                "blockedTime",
                "blockedTimeSlot",
                "bookedEvidenceOrPsnAppointment",
                "bulkRentChangeRequest",
                "calculatedDeductions",
                "calculateDeductions",
                "calculation",
                "calculationParts",
                "calculatorClaim",
                "capital",
                "carerCircumstances",
                "carerDeclaration",
                "carersAllowance",
                "carerTimeOff",
                "caringDeclaration",
                "caseLoadEvent",
                "checkAutoCalcPendingData",
                "childcare",
                "childcareDeclaration",
                "childcareTransactionData",
                "childDeclaration",
                "childElementEligibility",
                "childrenCircumstances",
                "childrenVerifiedCircumstancesConversation",
                "claimant",
                "claimantAccessType",
                "claimantAttachmentUpload",
                "claimantCommitment",
                "claimantCorporateAppointee",
                "claimantEligibilityDetails",
                "claimantEvent",
                "claimantHistory",
                "claimantIdentity",
                "claimantIdentityVerification",
                "claimantLanguagePreference",
                "claimantNotification",
                "claimantProfile",
                "claimantSpareRoomSubsidyEligibility",
                "claimantStats",
                "claimantUploadDetails",
                "claimantVerifyIdentity",
                "claimHistoryEntry",
                "coldWeatherPayment",
                "configuration",
                "confirmBenefitsDetailsToDoTimestamp",
                "contract",
                "contractEvent",
                "contractStats",
                "controlMeasure",
                "conversion-status",
                "corporateAppointeeBody",
                "corporateAppointeeBodyAddress",
                "corporateAppointeeBodyBankDetails",
                "corporateAppointeesTfaSmsTransactionMetadata",
                "corporateAppointeeUserAccount",
                "covid19ClaimSelectorLog",
                "covid19DeductionsBypassLog",
                "covid19PrepaymentBypassLog",
                "covidAffected",
                "covidAffectedToDo",
                "cpsPaymentFileRecord",
                "cyiClaimantRequest",
                "cyiLoginToken",
                "debtControlSheet",
                "debtControlSheetToDoData",
                "debtInterest",
                "debtPosition",
                "declaration",
                "declarationOverrideInterval",
                "deductionsStatus",
                "deductionsStatusRequired",
                "deferredConsiderClosingClaimDecision",
                "delayedMessage",
                "deliveryUnitAddress",
                "deliveryUnitThirdPartyReference",
                "depsAndNonDepsSpareRoomSubsidyEligibility",
                "destinationStatistics",
                "digimanData",
                "digimanDataArchive",
                "disclosure",
                "disclosureData",
                "earlyEstimatedCalculationParts",
                "earlyStatement",
                "earlyStatementContractId",
                "earningsData",
                "educationCircumstances",
                "educationDeclaration",
                "eligibilityCheckResponse",
                "emailSearch",
                "emailToClaimantMap",
                "emailVerificationToken",
                "employmentCircumstances",
                "encryptedData",
                "encryptedFileMetaData",
                "endOfAssessmentPeriodProcessStatus",
                "entitlementReason",
                "estimatedCalculation",
                "estimatedCalculationArchive",
                "estimatedCalculationParts",
                "estimatedDeductions",
                "expectedEvidenceDocuments",
                "extractJob",
                "extractSeqNo",
                "failedKafkaMessages",
                "failedMessage",
                "failedMigrationEncryptedData",
                "fasterPaymentStatus",
                "featureConfiguration",
                "featureToggles",
                "financialHardshipStatus",
                "firstCommitmentAppointmentsNotRequired",
                "fraudPenalty",
                "fraudPenaltyProgress",
                "gender",
                "genie",
                "gracePeriod",
                "guardiansAllowance",
                "healthAndDisabilityCircumstances",
                "healthAndDisabilityDeclaration",
                "healthCircumstances",
                "healthDeclaration",
                "historicRteData",
                "historicRteEarnings",
                "hmrcChildDataProvision",
                "holdingEncryptedData",
                "holdingMigrated",
                "housingCircumstances",
                "housingDeclaration",
                "housingEntitlement",
                "hrtAppointmentRequired",
                "idempotentNotificationMongoDocument",
                "identityVerificationDetails",
                "ineligiblePartner",
                "insolvencyOrder",
                "interaction",
                "interestPerson",
                "invalidUsernameIdentity",
                "ipAddressConfidence",
                "job",
                "jobStatus",
                "jounal",
                "journal",
                "ladsNotificationDetail",
                "landlordAddress",
                "landlordPortalTenancy",
                "landlordToDo",
                "landlordToDoLock",
                "languagePreference",
                "languagePreferenceDeclaration",
                "lastLogin",
                "lastSavedPrepareCommitments",
                "localUserMatchingData",
                "login",
                "managementInformation",
                "manualAdvancePayment",
                "manualAwardPayment",
                "manualDeductionsCalculationData",
                "manualOverride",
                "matchingServiceRequests",
                "moveToUc",
                "moveToUcClaim",
                "moveToUcRequest",
                "nationalCostsRuleReferenceData",
                "nationality",
                "nationalityDeclaration",
                "ninoCheck",
                "nonLandlordApaRequest",
                "nonStandardHousingAssessment",
                "notificationSettings",
                "notifyCisCache",
                "notifyThirdPartyDeductionToDoData",
                "onGoingInterest",
                "onlyOnceMongoDocument",
                "organisation",
                "otherBenefit",
                "otherBenefitAgentDeclaration",
                "otherBenefitRelatedAction",
                "otherBenefitsNotificationTimestamp",
                "otherIncome",
                "otherIncomeDeclaration",
                "outOfClaimDays",
                "overlappingBenefit",
                "overpayment",
                "partialDisclosure",
                "partnerQuestion",
                "partnerQuestionDeclaration",
                "payment",
                "paymentIC114827Backup",
                "paymentKeys",
                "paymentReference",
                "pendingAdvanceData",
                "pendingAuthorisationCache",
                "pendingNotification",
                "pendingStatement",
                "periodOfSickness",
                "persistedErrors",
                "personalDetails",
                "personDetails",
                "pinChallenge",
                "pinnedNote",
                "pregnancy",
                "pregnancyDeclaration",
                "prepaymentCheckFailure",
                "previousEarnings",
                "properties",
                "propertyAddress",
                "propertyAddressListUpdate",
                "proveYourIdentityTrial",
                "provideTenancyDetailsRequest",
                "qflowEnabledJobcentre",
                "quartz",
                "quartz_calendars",
                "quartz_jobs",
                "quartz_locks",
                "quartz_paused_trigger_groups",
                "quartz_schedulers",
                "quartz_triggers",
                "recalculateAgentStats",
                "recoverableHardshipPayment",
                "registrationTokenAllocation",
                "relationshipDeclaration",
                "reportSelfEmploymentEarningsFeedback",
                "requiredAuthorisationEscalation",
                "resolvedProperties",
                "rolePrivileges",
                "rteInterestSet",
                "rtiFeed",
                "saltData",
                "sanction",
                "sanctionEscalation",
                "sanctionProgress",
                "sanctionTerminationDecision",
                "savingsAndInvestmentDeclaration",
                "selfEmploymentLoss",
                "sentAppointmentReminder",
                "session",
                "sessionTraceable",
                "srsLandlordBankDetails",
                "srsRentChanges",
                "stagedNotification",
                "statement",
                "statementAndComparison",
                "stopTaxCreditNotice",
                "storedPassword",
                "summary",
                "supportForMortgageInterest",
                "supportMortgageInterestEligibilityDetails",
                "supportMortgageInterestLoanAccepted",
                "survey",
                "suspiciousCalculation",
                "systemWorkGroupAllocation",
                "team",
                "teamMemberStats",
                "telephonyClaimApplication",
                "telephonyClaimApplicationTransition",
                "temp",
                "temporaryAccess",
                "tenancyAgreement",
                "terminalIllness",
                "testCollection",
                "tfaDisabledLandlord",
                "tfaLastSuccessfulAttempt",
                "tfaSmsUser",
                "thirdParty",
                "thirdPartyAuthenticationTransactionMetadata",
                "thirdPartyCreditor",
                "thirdPartyDeduction",
                "thirdPartyDeductionPriority",
                "thirdPartyLandlordTransactionMetadata",
                "thirdPartyUsersTransactionMetadata",
                "threatLevel",
                "toDo",
                "traceable",
                "trackedNotification",
                "transactionResults",
                "transitionalProtection",
                "trialGroupParameters",
                "trialParticipant",
                "triggerHistory",
                "untrustedIdentity",
                "UpdateMongoLock_acceptedDataService",
                "UpdateMongoLock_advances",
                "UpdateMongoLock_agentAccessService",
                "UpdateMongoLock_agentAttachment",
                "UpdateMongoLock_agentCore",
                "UpdateMongoLock_agentSession",
                "UpdateMongoLock_appointments",
                "UpdateMongoLock_authenticator",
                "UpdateMongoLock_calculationProcess",
                "UpdateMongoLock_calculator",
                "UpdateMongoLock_childrenCore",
                "UpdateMongoLock_claimantAdvances",
                "UpdateMongoLock_claimantCommunicationService",
                "UpdateMongoLock_claimantHistory",
                "UpdateMongoLock_claimantSession",
                "UpdateMongoLock_claimantTfaSms",
                "UpdateMongoLock_coldWeatherPaymentsCore",
                "UpdateMongoLock_core",
                "UpdateMongoLock_corporateAppointeesCore",
                "UpdateMongoLock_corporateAppointeesTfaSms",
                "UpdateMongoLock_cryptoCore",
                "UpdateMongoLock_cryptoOps",
                "UpdateMongoLock_dataAcceptanceService",
                "UpdateMongoLock_dataArchivingCore",
                "UpdateMongoLock_deductions",
                "UpdateMongoLock_deductionsProcess",
                "UpdateMongoLock_hrtEntitlement",
                "UpdateMongoLock_identity",
                "UpdateMongoLock_laidPostcodeLookupCore",
                "UpdateMongoLock_landlordTfaSms",
                "UpdateMongoLock_matchingService",
                "UpdateMongoLock_moveToUcCore",
                "UpdateMongoLock_overpayments",
                "UpdateMongoLock_paymentsCore",
                "UpdateMongoLock_paymentsProcess",
                "UpdateMongoLock_penaltiesAndDeductions",
                "UpdateMongoLock_personalDetails",
                "UpdateMongoLock_propertyAddressLookupCore",
                "UpdateMongoLock_quartz",
                "UpdateMongoLock_riskDataService",
                "UpdateMongoLock_selfEmploymentClaimantCore",
                "UpdateMongoLock_sessionConfidence",
                "UpdateMongoLock_supportCore",
                "UpdateMongoLock_systemCore",
                "UpdateMongoLock_teamCore",
                "UpdateMongoLock_thirdParties",
                "UpdateMongoLock_thirdPartiesIntegration",
                "UpdateMongoLock_thirdPartyAuthentication",
                "UpdateMongoLock_thirdPartyLandlord",
                "UpdateMongoLock_thirdPartySession",
                "UpdateMongoLock_thirdPartyUsers",
                "UpdateMongoLock_token",
                "UpdateMongoLock_workCapabilityAssessment",
                "UpdateMongoLock_workSearchCore",
                "urlVisit",
                "userAssociation",
                "verifyIdentityAssessment",
                "watchList",
                "watchListMatches",
                "wizard",
                "workAndEarningsCircumstances",
                "workAndEarningsDeclaration",
                "workCapabilityAssessmentDecision",
                "workCapabilityAssessmentDeclaration",
                "workGroupAllocation",
                "youthObligationDetails")

        val coalesced = dumplist.map {
            hBaseWriter.coalesced(it)
        }.toSet()

        assertEquals(expected, coalesced)
    }

    @Test
    fun agentCoreAgentToDoArchiveIsCoalesced() {
        val actual = hBaseWriter.coalescedArchive("agent_core:agentToDoArchive")
        assertEquals("agent_core:agentToDo", actual)
    }

    @Test
    fun otherDbAgentToDoArchiveIsNotCoalesced() {
        val actual = hBaseWriter.coalescedArchive("other_db:agentToDoArchive")
        assertEquals("other_db:agentToDoArchive", actual)
    }


    @Test
    fun otherCollectionIsNotCoalesced() {
        val actual = hBaseWriter.coalescedArchive("core:calculationParts")
        assertEquals("core:calculationParts", actual)
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
    fun testPutBatchCallsFilterService() {
        val payload = (0..99).map {
            HBaseRecord("$it".toByteArray(), "$it-body".toByteArray(), it.toLong() + 100)
        }

        hBaseWriter.putBatch("database:collection", payload)
        verify(filterService, times(1)).nonExistent("database:collection", payload)
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
        given(messageProducer.produceMessage(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).willReturn("""{ "message": $json """)
        given(messageUtils.getTimestampAsLong(any())).willReturn(315532800000)
        given(messageUtils.parseJson(any())).willReturn(JsonObject(mapOf(Pair("key", "value"))))
        given(messageUtils.generateKeyFromRecordBody(any())).willReturn("FORMATTED_KEY".toByteArray())
        doNothing().whenever(hBaseWriter).ensureTable(any())
        ReflectionTestUtils.setField(hBaseWriter, "runMode", "import")
        given(filterService.filterStatus(any(), any(), any())).willReturn(FilterService.FilterStatus.DoNotFilter)
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
