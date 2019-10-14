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
import org.junit.Assert.assertEquals
import org.junit.Test
import org.slf4j.LoggerFactory

class MessageProducerTest {

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
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)

        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
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
        val mockAppender: Appender<ILoggingEvent> = mock()
        val logger = LoggerFactory.getLogger(MessageProducer::class.toString()) as ch.qos.logback.classic.Logger
        logger.addAppender(mockAppender)
        val captor = argumentCaptor<ILoggingEvent>()

        val actual = MessageProducer().produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)

        val expected = ""
        assertEquals(expected, actual)
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        val expectedMessage = """No '_lastModifiedDateTime' in record '{"idField":"$idFieldValue","anotherIdField":"$anotherIdFieldValue"}' from '$database/$collection'."""
        assertEquals(expectedMessage, formattedMessages[0])
    }

    @Test
    fun testRecordRejectedIfNoModifiedDate() {
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
        val captor = argumentCaptor<ILoggingEvent>()
        val actual = MessageProducer().produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)

        val expected = ""
        assertEquals(expected, actual)
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        val expectedMessage = """No '_lastModifiedDateTime' in record '{"idField":"$idFieldValue","anotherIdField":"$anotherIdFieldValue"}' from '$database/$collection'."""
        assertEquals(expectedMessage, formattedMessages[0])
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
        val expected = ""
        val mockAppender: Appender<ILoggingEvent> = mock()
        val logger = LoggerFactory.getLogger(MessageProducer::class.toString()) as ch.qos.logback.classic.Logger
        logger.addAppender(mockAppender)
        val captor = argumentCaptor<ILoggingEvent>()
        val actual = MessageProducer().produceMessage(jsonObject, encryptionResult, dataKeyResult, database, collection)
        assertEquals(expected, actual)
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        val expectedMessage = """No '_lastModifiedDateTime' in record '{"idField":"$idFieldValue","anotherIdField":"$anotherIdFieldValue"}' from '$database/$collection'."""
        assertEquals(expectedMessage, formattedMessages[0])
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