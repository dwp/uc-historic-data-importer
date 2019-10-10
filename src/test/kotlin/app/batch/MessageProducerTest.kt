package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.nhaarman.mockitokotlin2.mock
import junit.framework.Assert.assertEquals
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import org.junit.Test
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProducerTest {

    @Test
    fun testValidObjectGivesSchemaValidMessage() {
        val dateKey = "\$date"
        val dateValue = "2018-12-14T15:01:02.000+0000"
        val idFieldValue = "idFieldValue"
        val anotherIdFieldValue = "anotherIdFieldValue"

        val validJson = """{
            "_id": {
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            },
            "_lastModifiedDateTime": {
                "$dateKey": "$dateValue" 
            }
        }""".trimIndent()

        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(validJson)
        val jsonObject = parser.parse(stringBuilder) as JsonObject
        val initialisationVector = "initialisationVector"
        val encrypted = "encrypted"
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyEncryptionKeyId = "cloudhsm:1,2"
        val plaintextDataKey = "plaintextDataKey"
        val ciphertextDataKey = "ciphertextDataKey"
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val database = "database"
        val collection = "collection"
        val message = MessageProducer().produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val actual = parser.parse(StringBuilder(message)) as JsonObject
        validate(message)
        val expected = """{
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
    fun testValidObjectWithTypeGivesSchemaValidMessage() {
        val dateKey = "\$date"
        val dateValue = "2018-12-14T15:01:02.000+0000"
        val idFieldValue = "idFieldValue"
        val anotherIdFieldValue = "anotherIdFieldValue"
        val type = "type"
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
        val initialisationVector = "initialisationVector"
        val encrypted = "encrypted"
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyEncryptionKeyId = "cloudhsm:1,2"
        val plaintextDataKey = "plaintextDataKey"
        val ciphertextDataKey = "ciphertextDataKey"
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val database = "database"
        val collection = "collection"
        val message = MessageProducer().produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val actual = parser.parse(StringBuilder(message)) as JsonObject
        validate(message)
        val expected = """{
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

        val dateKey = "\$date"
        val dateValue = ""
        val idFieldValue = "idFieldValue"
        val anotherIdFieldValue = "anotherIdFieldValue"
        val type = "type"
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
        val initialisationVector = "initialisationVector"
        val encrypted = "encrypted"
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyEncryptionKeyId = "cloudhsm:1,2"
        val plaintextDataKey = "plaintextDataKey"
        val ciphertextDataKey = "ciphertextDataKey"
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val database = "database"
        val collection = "collection"
        val actual = MessageProducer().produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val expected = ""
        assertEquals(expected, actual)
    }

    @Test
    fun testRecordRejectedIfNoModifiedDate() {
        val idFieldValue = "idFieldValue"
        val anotherIdFieldValue = "anotherIdFieldValue"
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
        val initialisationVector = "initialisationVector"
        val encrypted = "encrypted"
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyEncryptionKeyId = "cloudhsm:1,2"
        val plaintextDataKey = "plaintextDataKey"
        val ciphertextDataKey = "ciphertextDataKey"
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val database = "database"
        val collection = "collection"
        val actual = MessageProducer().produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val expected = ""
        assertEquals(expected, actual)
    }

    @Test
    fun testRecordRejectedIfNoModifiedDateObject() {
        val idFieldValue = "idFieldValue"
        val anotherIdFieldValue = "anotherIdFieldValue"
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
        val initialisationVector = "initialisationVector"
        val encrypted = "encrypted"
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyEncryptionKeyId = "cloudhsm:1,2"
        val plaintextDataKey = "plaintextDataKey"
        val ciphertextDataKey = "ciphertextDataKey"
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val database = "database"
        val collection = "collection"
        val actual = MessageProducer().produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        val expected = ""
        val mockAppender: Appender<ILoggingEvent> = mock()
        val logger: Logger = LoggerFactory.getLogger(MessageProducer::class.toString())
        //logger.add
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
}