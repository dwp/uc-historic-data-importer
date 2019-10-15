package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
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

    @Autowired
    private lateinit var messageProducer: MessageProducer

    @Autowired
    private lateinit var messageUtils: MessageUtils

    private val filenamePattern = """(?<database>[a-z0-9-]+)\.(?<collection>[a-z0-9-]+)\.\d+\.json\.gz\.enc$"""
    private val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE)

    override fun write(items: MutableList<out DecompressedStream>) {
        items.forEach {
            logger.info("Processing '${it.fileName}'.")
            val fileName = it.fileName
            val matchResult = filenameRegex.find(fileName)
            if (matchResult != null) {
                val groups = matchResult.groups
                val database = groups[1]!!.value // can assert nun-null as it matched on the regex
                val collection = groups[2]!!.value // ditto
                val dataKeyResult: DataKeyResult = getDataKey(fileName)

                var lineNo = 0;
                BufferedReader(InputStreamReader(it.inputStream)).forEachLine { line ->
                    lineNo++
                    try {
                        val json = messageUtils.parseJson(line)
                        val id = messageUtils.getId(json)?.toJsonString()
                        if (StringUtils.isNotBlank(id)) {
                            val encryptionResult = encryptDbObject(dataKeyResult, line, fileName, id)
                            val message = messageProducer.produceMessage(json, encryptionResult, dataKeyResult,
                                    database, collection)
                            val lastModifiedTimestampStr = messageUtils.getLastModifiedTimestamp(json)
                            if (StringUtils.isNotBlank(lastModifiedTimestampStr)) {
                                val lastModifiedTimestampLong = messageUtils.getTimestampAsLong(lastModifiedTimestampStr)
                                val formattedkey = messageUtils.generateKeyFromRecordBody(json)
                                val topic = "$database.$collection"
                                try {
                                    hbase.putVersion(
                                            topic = topic.toByteArray(), // TODO what topic we should insert
                                            key = formattedkey,
                                            body = message.toByteArray(),
                                            version = lastModifiedTimestampLong
                                    )
                                    logger.info("Written id $id as key  $formattedkey to HBase.")
                                } catch (e: Exception) {
                                    logger.error("Error writing record to HBase with id $id as key  $formattedkey to HBase.")
                                    throw e
                                }
                            } else {
                                logger.info("Skipping record $lineNo in the file $fileName due to absence of lastModifiedTimeStamp")
                            }

                        } else {
                            logger.info("Skipping record $lineNo in the file $fileName due to absence of id")
                        }

                    } catch (e: Exception) {
                        logger.error("Error while parsing record $lineNo from '$fileName': '${e.message}'.")
                    }
                    logger.info("Processed file $fileName")

                }
            }
        }
    }

    fun encryptDbObject(dataKeyResult: DataKeyResult, line: String, fileName: String, id: String?): EncryptionResult {
        try {
            return cipherService.encrypt(dataKeyResult.plaintextDataKey, line.toByteArray())
        } catch (e: Exception) {
            logger.error("Error while encrypting db object id $id in file  ${fileName}: $e")
            throw e
        }
    }

    fun getDataKey(fileName: String): DataKeyResult {
        try {
            return keyService.batchDataKey()
        } catch (e: Exception) {
            logger.error("Error while creating data key for the file  $fileName: $e")
            throw e
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }
}
