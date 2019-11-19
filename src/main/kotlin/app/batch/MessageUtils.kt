package app.batch

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.beust.klaxon.lookup
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.nio.ByteBuffer
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.zip.CRC32

@Component
class MessageUtils {
    val logger: Logger = LoggerFactory.getLogger(MessageUtils::class.toString())
    val EPOCH = "1980-01-01T00:00:00.000Z"

    fun getTimestampAsLong(timeStampAsStr: String?): Long {
        val validTimestamps = listOf(
                "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
                "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"
            )

        validTimestamps.forEach {
            try {
                val df = SimpleDateFormat(it)
                return df.parse(timeStampAsStr).time
            }
            catch (e: Exception) {
                logger.debug("'$timeStampAsStr' did not match date format '$it'")
            }
        }
        throw ParseException("Unparseable date: '$timeStampAsStr'", 0)
    }

    fun getLastModifiedTimestamp(json: JsonObject?): String? {
        val lastModified = json?.lookup<String?>("message._lastModifiedDateTime")?.get(0)

        if (lastModified != null) {
            return lastModified
        }

        logger.warn("No _lastModifiedDateTime in message defaulting to '$EPOCH'.")
        return EPOCH
    }

    fun generateKeyFromRecordBody(body: JsonObject?): ByteArray {
        val id: JsonObject? = body?.let { getId(it) }
        return if (id == null) ByteArray(0) else generateKey(id)
    }

    fun getId(json: JsonObject): JsonObject? {
        try {
            val message: JsonObject? = json.obj("message")
            return if (message == null) null else message.obj("_id")
        } catch (e: Exception) {
            logger.warn("Message does not contain valid json object with  _id field")
            return null
        }
    }

    fun getIdFromDbObject(json: JsonObject): JsonObject? {
        try {
            return json.obj("_id")
        } catch (e: Exception) {
            logger.warn("DB Object does not contain valid json object with  _id field")
            return null
        }
    }

    fun parseJson(line: String?): JsonObject {
        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(line)
        return parser.parse(stringBuilder) as JsonObject
    }

    fun generateKey(json: JsonObject): ByteArray {
        val jsonOrdered = sortJsonByKey(json)
        val checksumBytes: ByteArray = generateFourByteChecksum(jsonOrdered)

        return checksumBytes.plus(jsonOrdered.toByteArray())
    }

    fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy { it })
        val json: JsonObject = JsonObject(sortedEntries)
        return json.toJsonString()
    }

    fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array()
    }
}
