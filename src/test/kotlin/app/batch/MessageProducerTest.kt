package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import app.utils.logging.overrideLoggerStaticFieldsForTests
import app.utils.logging.resetLoggerStaticFieldsForTests
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.nhaarman.mockitokotlin2.mock
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.text.SimpleDateFormat
import java.util.*

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [MessageProducer::class])
@TestPropertySource(properties = [
    "trace.id=1", "hdi.version=1.0.0"
])
class MessageProducerTest {

    @Autowired
    private lateinit var messageProducer: MessageProducer

    private val dateKey = "\$date"
    private val dateValue = "2018-12-14T15:01:02.000+0000"
    private val idFieldValue = "idFieldValue"
    private val anotherIdFieldValue = "anotherIdFieldValue"
    private val initialisationVector = "initialisationVector"
    private val encrypted = "encrypted"
    private val dataKeyEncryptionKeyId = "cloudhsm:1,2"
    private val plaintextDataKey = "plaintextDataKey"
    private val ciphertextDataKey = "ciphertextDataKey"
    private val database = "database"
    private val collection = "collection"
    private val type = "type"

    @Before
    fun setup() {
        overrideLoggerStaticFieldsForTests("test-host", "test-env", "my-app", "v1", "tests", "9876543000", "correlation1")
    }

    @After
    fun tearDown() {
        resetLoggerStaticFieldsForTests()
    }

    @Test
    fun testValidObjectGivesSchemaValidMessage() {

        val validJson = validJsonOne()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val id = Gson().toJson(jsonObject.getAsJsonObject("_id"))
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject!!, id, false, false, dateValue, false, false, true, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")
        validate(message)
        val expected = """{
              "traceId": "correlation1",
              "@type": "HDI",
              "version": "1.0.0",
              "message": {
                "@type": "MONGO_IMPORT",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": false,
                "created_date_time_was_altered": false,
                "removed_date_time_was_altered": true,
                "historic_removed_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateValue",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

        val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
        assertEquals("Expected: '$expectedObject', Actual: '$actual'", expectedObject, actual)
    }

    @Test
    fun testRemovedObjectGetsFlagAndType() {

        val validJson = validDelete()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val id = Gson().toJson(jsonObject.getAsJsonObject("_id"))
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject!!, id, false, false, dateValue, false, false, true, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")
        validate(message)
        val expected = """{
              "traceId": "correlation1",
              "@type": "HDI",
              "version": "1.0.0",
              "message": {
                "@type": "MONGO_DELETE",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": false,
                "created_date_time_was_altered": false,
                "removed_date_time_was_altered": true,
                "historic_removed_record_altered_on_import": true,
                "_lastModifiedDateTime": "$dateValue",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

        val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
        assertEquals("Expected: '$expectedObject', Actual: '$actual'", expectedObject, actual)
    }

    @Test
    fun testModifiedFieldsReflectedInMessage() {

        val validJson = validJsonOne()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val dateTime = "2019-11-13T14:02:03.000+0000"
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject!!, idFieldValue, true, false, dateTime, true, true, false, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")
        validate(message)
        val expected = """{
              "traceId": "correlation1",
              "@type": "HDI",
              "version": "1.0.0",
              "message": {
                "@type": "MONGO_IMPORT",
                "_id": "$idFieldValue",
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": true,
                "created_date_time_was_altered": true,
                "removed_date_time_was_altered": false,
                "historic_removed_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateTime",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

        val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
        assertEquals("Expected: '$expectedObject', Actual: '$actual'", expectedObject, actual)
    }

    @Test
    fun testModifiedFieldsReflectedInMessageWithStringId() {

        val validJson = validJsonWithStringId()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val id = "AN_ID"
        val dateTime = "2019-11-13T14:02:03.000+0000"
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject!!, id, true, true, dateTime, true, true, false, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")
        validate(message)
        val expected = """{
              "traceId": "correlation1",
              "@type": "HDI",
              "version": "1.0.0",
              "message": {
                "@type": "MONGO_IMPORT",
                "_id": "$id",
                "mongo_format_stripped_from_id": true,
                "last_modified_date_time_was_altered": true,
                "created_date_time_was_altered": true,
                "removed_date_time_was_altered": false,
                "historic_removed_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateTime",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

        val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
        assertEquals("Expected: '$expectedObject', Actual: '$actual'", expectedObject, actual)
    }

    @Test
    fun testTwoObjectsGetSameGuid() =
        assertEquals(unitOfWorkId(validJsonOne()), unitOfWorkId(validJsonTwo()))

    @Test
    fun testTimestampRepresentsTimeOfMessageCreation() {
        val start = Date()
        val timestamp = timestamp(validJsonOne())
        val date = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(timestamp)
        val end = Date()
        assertTrue(start.before(date) || start.equals(date))
        assertTrue(end.after(date) || end.equals(date))
    }

    @Test
    fun testValidObjectWithTypeGivesSchemaValidMessage() {

        val id = """{
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            }"""

        val validJson = """{
            "_id": $id,
            "@type": "$type",
            "_lastModifiedDateTime": {
                "$dateKey": "$dateValue" 
            }
        }""".trimIndent()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject, id, false, false, dateValue, false, false, false, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")
        validate(message)
        val expected = """{
              "traceId": "correlation1",
              "@type": "HDI",
              "version": "1.0.0",
              "message": {
                "@type": "$type",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": false,
                "created_date_time_was_altered": false,
                "removed_date_time_was_altered": false,
                "historic_removed_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateValue",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

        val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
        assertEquals("Expected: '$expectedObject', Actual: '$actual'", expectedObject, actual)
    }

    private fun validate(json: String) = schemaLoader().load().build().validate(JSONObject(json))

    private fun schemaLoader() =
        SchemaLoader.builder()
            .schemaJson(schemaObject())
            .draftV7Support()
            .build()

    private fun schemaObject() =
        javaClass.getResourceAsStream("/message.schema.json")
            .use { inputStream ->
                JSONObject(JSONTokener(inputStream))
            }

    private fun validJsonTwo(): String {
        return """{
                "_id": {
                    "idField": "$anotherIdFieldValue",
                    "anotherIdField": "$idFieldValue"
                },
                "_lastModifiedDateTime": "$dateValue" 
            }""".trimIndent()
    }

    private fun validJsonOne(): String {
        return """{
                "_id": {
                    "idField": "$idFieldValue",
                    "anotherIdField": "$anotherIdFieldValue"
                },
                "_lastModifiedDateTime": "$dateValue" 
            }""".trimIndent()
    }

    private fun validJsonWithStringId(): String {
        return """{
                "_id": "$idFieldValue",
                "_lastModifiedDateTime": "$dateValue" 
            }""".trimIndent()
    }

    private fun validDelete(): String {
        return """{
                "_id": {
                    "idField": "$idFieldValue",
                    "anotherIdField": "$anotherIdFieldValue"
                },
                "@type": "MONGO_DELETE",
                "_lastModifiedDateTime": "$dateValue" 
            }""".trimIndent()
    }

    private fun unitOfWorkId(json: String) = messageField("unitOfWorkId", json)
    private fun timestamp(json: String) = messageField("timestamp", json)

    private fun messageField(field: String, json: String): String {
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val jsonObjectOne = Gson().fromJson(json, JsonObject::class.java)
        val id = Gson().toJson(jsonObjectOne.getAsJsonObject("_id"))
        val messageOne = messageProducer.produceMessage(jsonObjectOne, id, false, false, dateValue, false, false, false, encryptionResult, dataKeyResult, database, collection)
        val actualOne = Gson().fromJson(messageOne, JsonObject::class.java)
        return actualOne.getAsJsonPrimitive(field).asString
    }

}
