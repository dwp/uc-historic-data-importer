package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.nhaarman.mockitokotlin2.mock
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import org.junit.Assert.*
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

    @Test
    fun testValidObjectGivesSchemaValidMessage() {

        val validJson = validJsonOne()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val id = Gson().toJson(jsonObject.getAsJsonObject("_id"))
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject!!, id, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")
        validate(message)
        val expected = """{
              "traceId": "1",
              "@type": "HDI",
              "version": "1.0.0",
              "message": {
                "@type": "MONGO_UPDATE",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
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
        assertEquals(expectedObject, actual)
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
        val message = messageProducer.produceMessage(jsonObject, id, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")
        validate(message)
        val expected = """{
              "traceId": "1",
              "@type": "HDI",
              "version": "1.0.0",
              "message": {
                "@type": "$type",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
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
        assertEquals(expectedObject, actual)
    }

    @Test
    fun testEpochUsedIfEmptyModifiedDate() {

        val id = """{
            "idField": "$idFieldValue",
            "anotherIdField": "$anotherIdFieldValue"
        }"""

        val validJson = """{
            "_id": $id,
            "@type": "$type",
            "_lastModifiedDateTime": {
                "$dateKey": "" 
            }
        }""".trimIndent()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)

        val message = messageProducer.produceMessage(jsonObject, id, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")

        validate(message)

        val expected = """{
              "traceId": "1",
              "@type": "HDI",
              "version": "1.0.0",
              "message": {
                "@type": "$type",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
                "_lastModifiedDateTime": "1980-01-01T00:00:00.000Z",
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
        assertEquals(expectedObject, actual)
    }

    @Test
    fun testEpochUsedIfNoModifiedDate() {
        val type = "type"

        val id = """{
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            }"""

        val validJson = """{
            "_id": $id,
            "@type": "$type",
            "_lastModifiedDateTime": {
            }
        }""".trimIndent()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val mockAppender: Appender<ILoggingEvent> = mock()
        val logger = LoggerFactory.getLogger(MessageProducer::class.toString()) as ch.qos.logback.classic.Logger
        logger.addAppender(mockAppender)
        val message = messageProducer.produceMessage(jsonObject, id, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)

        val expectedMessage = """{
           "traceId": "1",
           "@type": "HDI",
           "version": "1.0.0",
           "message": {
               "@type": "type",
               "_id": {"idField":"idFieldValue","anotherIdField":"anotherIdFieldValue"},
               "_lastModifiedDateTime": "1980-01-01T00:00:00.000Z",
               "collection" : "collection",
               "db": "database",
               "dbObject": "encrypted",
               "encryption": {
                   "keyEncryptionKeyId": "cloudhsm:1,2",
                   "initialisationVector": "initialisationVector",
                   "encryptedEncryptionKey": "ciphertextDataKey"
               }
           }
        }"""
        val expected = Gson().fromJson(expectedMessage, JsonObject::class.java)

        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")
        assertEquals(expected, actual)
    }

    @Test
    fun testRecordRejectedIfNoModifiedDateObject() {
        val type = "type"
        val id = """{
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            }"""

        val validJson = """{
            "_id": $id,
            "@type": "$type"
        }""".trimIndent()

        val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject, id, encryptionResult, dataKeyResult, database, collection)
        val actual = Gson().fromJson(message, JsonObject::class.java)
        val unitOfWorkId = actual["unitOfWorkId"]
        val timestamp = actual["timestamp"]
        assertNotNull(unitOfWorkId)
        assertNotNull(timestamp)
        actual.remove("unitOfWorkId")
        actual.remove("timestamp")

        val expectedMessage = """{
           "traceId": "1",
           "@type": "HDI",
           "version": "1.0.0",
           "message": {
               "@type": "type",
               "_id": {"idField":"idFieldValue","anotherIdField":"anotherIdFieldValue"},
               "_lastModifiedDateTime": "1980-01-01T00:00:00.000Z",
               "collection" : "collection",
               "db": "database",
               "dbObject": "encrypted",
               "encryption": {
                   "keyEncryptionKeyId": "cloudhsm:1,2",
                   "initialisationVector": "initialisationVector",
                   "encryptedEncryptionKey": "ciphertextDataKey"
               }
           }
        }"""
        val expected = Gson().fromJson(expectedMessage, JsonObject::class.java)

        assertEquals(expected, actual)
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
                "_lastModifiedDateTime": {
                    "$dateKey": "$dateValue" 
                }
            }""".trimIndent()
    }

    private fun validJsonOne(): String {
        return """{
                "_id": {
                    "idField": "$idFieldValue",
                    "anotherIdField": "$anotherIdFieldValue"
                },
                "_lastModifiedDateTime": {
                    "$dateKey": "$dateValue" 
                }
            }""".trimIndent()
    }

    private fun unitOfWorkId(json: String) = messageField("unitOfWorkId", json)
    private fun timestamp(json: String) = messageField("timestamp", json)

    private fun messageField(field: String, json: String): String {
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val jsonObjectOne = Gson().fromJson(json, JsonObject::class.java)
        val id = Gson().toJson(jsonObjectOne.getAsJsonObject("_id"))
        val messageOne = messageProducer.produceMessage(jsonObjectOne, id, encryptionResult, dataKeyResult, database, collection)
        val actualOne = Gson().fromJson(messageOne, JsonObject::class.java)
        return actualOne.getAsJsonPrimitive(field).asString
    }

}
