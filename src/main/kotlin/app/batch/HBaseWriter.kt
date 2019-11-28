package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.domain.ManifestRecord
import app.services.CipherService
import app.services.KeyService
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
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

    @Autowired
    private lateinit var manifestWriter: ManifestWriter

    @Value("\${kafka.topic.prefix:db}")
    private lateinit var kafkaTopicPrefix: String

    @Value("\${run-mode:import_and_manifest}")
    private lateinit var runMode: String

    private val filenamePattern = """(?<database>[\w-]+)\.(?<collection>[[\w-]+]+)\.(?<filenumber>[0-9]+)\.json\.gz\.enc$"""
    private val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE)

    private val RUN_MODE_MANIFEST = "manifest"
    private val RUN_MODE_IMPORT = "import"

    override fun write(items: MutableList<out DecompressedStream>) {
        val cpus = Runtime.getRuntime().availableProcessors()
        logger.info("AVAILABLE PROCESSORS: $cpus")
        items.forEach {
            logger.info("Processing '${it.fileName}'.")
            val fileName = it.fileName
            val matchResult = filenameRegex.find(fileName)
            if (matchResult != null) {
                val groups = matchResult.groups
                val database = groups[1]!!.value // can assert nun-null as it matched on the regex
                val collection = groups[2]!!.value // ditto
                val fileNumber = groups[3]!!.value
                val dataKeyResult: DataKeyResult = getDataKey(fileName)
                var lineNo = 0
                val manifestRecords = mutableListOf<ManifestRecord>()
                BufferedReader(InputStreamReader(it.inputStream)).forEachLine { line ->
                    lineNo++
                    try {
                        val json = messageUtils.parseJson(line)
                        val id = messageUtils.getIdFromDbObject(json)?.toJsonString()

                        if (StringUtils.isBlank(id)) {
                            logger.info("Skipping record $lineNo in the file $fileName due to absence of id")
                            return@forEachLine
                        }

                        val encryptionResult = encryptDbObject(dataKeyResult, line, fileName, id)
                        val message = messageProducer.produceMessage(json, encryptionResult, dataKeyResult,
                            database, collection)
                        val messageJsonObject = messageUtils.parseJson(message)
                        val lastModifiedTimestampStr = messageUtils.getLastModifiedTimestamp(messageJsonObject)

                        if (StringUtils.isBlank(lastModifiedTimestampStr)) {
                            logger.info("Skipping record $lineNo in the file $fileName due to absence of lastModifiedTimeStamp")
                            return@forEachLine
                        }

                        val lastModifiedTimestampLong = messageUtils.getTimestampAsLong(lastModifiedTimestampStr)
                        val formattedKey = messageUtils.generateKeyFromRecordBody(messageJsonObject)
                        val formattedKeyString = formattedKey.contentToString()
                        logger.info("Formatted key for the record '$id' is '$formattedKeyString'")

                        val topic = "$kafkaTopicPrefix.$database.$collection"
                        if (runMode != RUN_MODE_MANIFEST) {
                            hbase.putVersion(
                                topic = topic.toByteArray(),
                                key = formattedKey,
                                body = message.toByteArray(),
                                version = lastModifiedTimestampLong
                            )
                            logger.info("Written record $lineNo id $id as key $formattedKey to HBase topic $topic.")
                        }

                        val type = messageUtils.getType(messageJsonObject)
                        if (runMode != RUN_MODE_IMPORT) {
                            val manifestRecord = ManifestRecord(id!!, lastModifiedTimestampLong, database, collection, "IMPORT", type)
                            manifestRecords.add(manifestRecord)
                        }
                    } catch (e: Exception) {
                        logger.error("Error processing record $lineNo from '$fileName': '${e.message}'.")
                    }

                    logger.info("Processed $lineNo records in the file $fileName")

                }
                if (runMode != RUN_MODE_IMPORT) {
                    manifestWriter.generateManifest(manifestRecords, database, collection, fileNumber)
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
