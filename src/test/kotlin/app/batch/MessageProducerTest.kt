package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
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

        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(validJson)
        val jsonObject = parser.parse(stringBuilder) as JsonObject
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val actual = parser.parse(StringBuilder(message)) as JsonObject
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

        val expectedObject = parser.parse(StringBuilder(expected)) as JsonObject
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
        val validJson = """{
            "_id": {
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            },
            "@type": "$type",
            "_lastModifiedDateTime": {
                "$dateKey": "$dateValue" 
            }
        }""".trimIndent()

        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(validJson)
        val jsonObject = parser.parse(stringBuilder) as JsonObject
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val actual = parser.parse(StringBuilder(message)) as JsonObject
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

        val expectedObject = parser.parse(StringBuilder(expected)) as JsonObject
        assertEquals(expectedObject, actual)
    }

    @Test
    fun testRecordRejectedIfEmptyModifiedDate() {

        val validJson = """{
            "_id": {
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            },
            "@type": "$type",
            "_lastModifiedDateTime": {
                "$dateKey": "" 
            }
        }""".trimIndent()

        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(validJson)
        val jsonObject = parser.parse(stringBuilder) as JsonObject
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)

        val message = messageProducer.produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val actual = parser.parse(StringBuilder(message)) as JsonObject
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
                "_lastModifiedDateTime": "1980-01-01T00:00:00.000",
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

        val expectedObject = parser.parse(StringBuilder(expected)) as JsonObject
        assertEquals(expectedObject, actual)
    }

    @Test
    fun testEpochUsedIfNoModifiedDate() {
        val type = "type"
        val validJson = """{
            "_id": {
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            },
            "@type": "$type",
            "_lastModifiedDateTime": {
            }
        }""".trimIndent()

        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(validJson)
        val jsonObject = parser.parse(stringBuilder) as JsonObject
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val mockAppender: Appender<ILoggingEvent> = mock()
        val logger = LoggerFactory.getLogger(MessageProducer::class.toString()) as ch.qos.logback.classic.Logger
        logger.addAppender(mockAppender)
        val message = messageProducer.produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val actual = parser.parse(StringBuilder(message)) as JsonObject

        val expectedMessage = """{
           "traceId": "1",
           "@type": "HDI",
           "version": "1.0.0",
           "message": {
               "@type": "type",
               "_id": {"idField":"idFieldValue","anotherIdField":"anotherIdFieldValue"},
               "_lastModifiedDateTime": "1980-01-01T00:00:00.000",
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
        val expected = parser.parse(StringBuilder(expectedMessage)) as JsonObject

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
        val validJson = """{
            "_id": {
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            },
            "@type": "$type"
        }""".trimIndent()

        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(validJson)
        val jsonObject = parser.parse(stringBuilder) as JsonObject
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val message = messageProducer.produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val actual = parser.parse(StringBuilder(message)) as JsonObject
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
               "_lastModifiedDateTime": "1980-01-01T00:00:00.000",
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
        val expected = parser.parse(StringBuilder(expectedMessage)) as JsonObject

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
        val parser: Parser = Parser.default()
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val jsonObjectOne = parser.parse(StringBuilder(json)) as JsonObject
        val messageOne = messageProducer.produceMessage(jsonObjectOne, encryptionResult, dataKeyResult, database, collection)
        val actualOne = parser.parse(StringBuilder(messageOne)) as JsonObject
        return actualOne[field] as String
    }

}
