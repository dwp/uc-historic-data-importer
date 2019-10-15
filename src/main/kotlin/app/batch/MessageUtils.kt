package app.batch

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.beust.klaxon.lookup
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.nio.ByteBuffer
import java.text.SimpleDateFormat
import java.util.zip.CRC32

@Component
open class MessageUtils {
    val logger: Logger = LoggerFactory.getLogger(MessageUtils::class.toString())

    fun getTimestampAsLong(timeStampAsStr: String?, timeStampPattern: String = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"): Long {
        val df = SimpleDateFormat(timeStampPattern);
        return df.parse(timeStampAsStr).time
    }

    fun getLastModifiedTimestamp(json: JsonObject?): String? {
        try {
            val lastModifiedTimestampStr = json?.lookup<String?>("message._lastModifiedDateTime.\$date")?.get(0)
            return lastModifiedTimestampStr
        } catch (e: Exception) {
            logger.warn("Record body does not contain valid json object with  _lastModifiedDateTime field")
            return null
        }
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
            logger.warn("Record body does not contain valid json object with  _id field")
            return null
        }
    }

    fun parseJson(line: String?): JsonObject {
        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(line)
        val json = parser.parse(stringBuilder) as JsonObject
        return json
    }

    fun generateKey(json: JsonObject): ByteArray {
        val jsonOrdered = sortJsonByKey(json)
        val checksumBytes: ByteArray = generateFourByteChecksum(jsonOrdered)

        return checksumBytes.plus(jsonOrdered.toByteArray())
    }

    fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy<String> { it })
        val json: JsonObject = JsonObject(sortedEntries)
        return json.toJsonString()
    }

    fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.getValue().toInt()).array();
    }
}
