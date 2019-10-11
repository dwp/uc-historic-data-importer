package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.stereotype.Component
import java.io.BufferedReader
import java.io.InputStreamReader

@Component
class HBaseWriter : ItemWriter<DecompressedStream> {

    @Autowired
    private lateinit var cipherService: CipherService

    @Autowired
    private lateinit var keyService: KeyService

    @Autowired
    private lateinit var hbase: HbaseClient

    override fun write(items: MutableList<out DecompressedStream>) {
        items.forEach {
            val fileName = it.fileName
            val filenamePattern = """(?<database>[a-z-]+)\.(?<collection>[a-z-]+)\.\d+\.json\.gz\.enc$"""
            val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE)
            val matchResult = filenameRegex.find(fileName)
            if (matchResult != null) {
                val groups = matchResult.groups
                val database = groups[1]!!.value // can assert nun-null as it matched on the regex
                val collection = groups[2]!!.value // ditto
                val dataKeyResult: DataKeyResult = getDataKey(fileName)
                val reader = BufferedReader(InputStreamReader(it.inputStream))
                var line: String? = null
                while ({ line = reader.readLine(); line }() != null) {
                    try {
                        val parser: Parser = Parser.default()
                        val stringBuilder = StringBuilder(line)
                        val json = parser.parse(stringBuilder) as JsonObject
                        val id = MessageUtils.getId(json)?.toJsonString()
                        if (StringUtils.isNotBlank(id)) {
                            val encryptionResult = encryptDbObject(dataKeyResult, line!!, fileName, id)
                            logger.info("result: '$encryptionResult', fileName: '$fileName'.")
                            val message = MessageProducer().produceMessage(json, encryptionResult, dataKeyResult,
                                                                            database, collection)
                            val lastModifiedTimestampStr = MessageUtils.getLastModifiedTimestamp(json)
                            val lastModifiedTimestampLong = MessageUtils.getTimestampAsLong(lastModifiedTimestampStr)
                            val formattedkey = MessageUtils.generateKeyFromRecordBody(json)
                            try {
                                hbase.putVersion(
                                    topic = "$database.$collection".toByteArray(), // TODO what topic we should insert
                                    key = formattedkey,
                                    body = message.toByteArray(),
                                    version = lastModifiedTimestampLong
                                )
                                logger.info("Written id $id as key  $formattedkey to HBase.")
                            } catch (e: Exception) {
                                logger.error("Error writing record to HBase with id $id as key  $formattedkey to HBase.")
                                throw e
                            }

                            logger.info("Message: '$message'.")
                        }
                    }
                    catch (e: Exception) {
                        logger.error("Error while parsing record from '$fileName': '${e.message}'.", e)
                    }
                }
            }
        }
    }

    fun encryptDbObject(dataKeyResult: DataKeyResult, line: String, fileName: String, id: String?): EncryptionResult {
        try {
            return cipherService.encrypt(dataKeyResult.plaintextDataKey, line.toByteArray())
        }
        catch (e: Exception) {
            logger.error("Error while encrypting db object id $id in file  ${fileName}: $e")
            throw e
        }
    }

    fun getDataKey(fileName: String): DataKeyResult {
        try {
            return keyService.batchDataKey()
        }
        catch (e: Exception) {
            logger.error("Error while creating data key for the file  $fileName: $e")
            throw e
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }
}
