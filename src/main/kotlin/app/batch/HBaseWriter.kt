package app.batch

import app.domain.*
import app.services.CipherService
import app.services.KeyService
import com.amazonaws.services.s3.AmazonS3
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.*

@Component
class HBaseWriter : ItemWriter<DecompressedStream> {

    @Autowired
    private lateinit var s3: AmazonS3

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

    @Value("\${kafka.topic.prefix:db}")
    private lateinit var kafkaTopicPrefix: String

    @Value("\${manifest.output.directory:.}")
    private lateinit var manifestOutputDirectory: String

    @Value("\${hbase.batch.size:10000}")
    private lateinit var hbaseBatchSize: String

    @Value("\${max.stream.attempts:10}")
    private lateinit var maxStreamAttempts: String

    @Value("\${run-mode:import_and_manifest}")
    private lateinit var runMode: String

    @Value("\${s3.manifest.prefix.folder}")
    private lateinit var manifestPrefix: String

    @Value("\${s3.manifest.bucket}")
    private lateinit var manifestBucket: String

    @Value("\${s3.bucket}")
    private lateinit var s3bucket: String

    private val filenamePattern = """(?<database>[\w-]+)\.(?<collection>[[\w-]+]+)\.(?<filenumber>[0-9]+)\.json\.gz\.enc$"""
    private val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE)

    private val RUN_MODE_MANIFEST = "manifest"
    private val RUN_MODE_IMPORT = "import"

    override fun write(items: MutableList<out DecompressedStream>) {
        val cpus = Runtime.getRuntime().availableProcessors()
        logger.info("AVAILABLE PROCESSORS: $cpus, runMode: '$runMode'.")
        items.forEach { input ->
            logger.info("Processing '${input.fileName}'.")
            val fileName = input.fileName
            val matchResult = filenameRegex.find(fileName)
            if (matchResult != null) {
                var batch = mutableListOf<HBaseRecord>()
                val maxBatchSize = hbaseBatchSize.toInt()
                val groups = matchResult.groups
                val database = groups[1]!!.value // can assert nun-null as it matched on the regex
                val collection = groups[2]!!.value // ditto
                val fileNumber = groups[3]!!.value
                val dataKeyResult: DataKeyResult = getDataKey(fileName)
                var lineNo = 0
                val gson = Gson()
                val manifestWriter = StreamingManifestWriter()
                val manifestOutputFile = "${manifestOutputDirectory}/${manifestWriter.topicName(database, collection)}-%06d.csv".format(fileNumber.toInt())
                BufferedWriter(OutputStreamWriter(FileOutputStream(manifestOutputFile))).use { writer ->
                    var succeeded = false
                    var attempts = 0
                    var inputStream = input.inputStream
                    while (!succeeded && attempts < maxStreamAttempts.toInt()) {
                        try {
                            ++attempts
                            getBufferedReader(inputStream).forEachLine { line ->
                                ++lineNo
                                try {
                                    val json = messageUtils.parseGson(line)
                                    val id = gson.toJson(idObject(json))

                                    if (StringUtils.isBlank(id) || id == "null") {
                                        logger.warn("Skipping record $lineNo in the file $fileName due to absence of id")
                                        return@forEachLine
                                    }

                                    val encryptionResult = encryptDbObject(dataKeyResult, line, fileName, id)
                                    val message = messageProducer.produceMessage(json, id, encryptionResult, dataKeyResult,
                                            database, collection)
                                    val messageJsonObject = messageUtils.parseJson(message)
                                    val lastModifiedTimestampStr = messageUtils.getLastModifiedTimestamp(messageJsonObject)

                                    if (StringUtils.isBlank(lastModifiedTimestampStr)) {
                                        logger.warn("Skipping record $lineNo in the file $fileName due to absence of lastModifiedTimeStamp")
                                        return@forEachLine
                                    }

                                    val lastModifiedTimestampLong = messageUtils.getTimestampAsLong(lastModifiedTimestampStr)
                                    val formattedKey = messageUtils.generateKeyFromRecordBody(messageJsonObject)

                                    val topic = "$kafkaTopicPrefix.$database.$collection"
                                    if (runMode != RUN_MODE_MANIFEST) {
                                        batch.add(HBaseRecord(topic.toByteArray(),
                                                formattedKey,
                                                message.toByteArray(),
                                                lastModifiedTimestampLong))

                                        if (batch.size >= maxBatchSize) {
                                            try {
                                                hbase.putBatch(batch)
                                                logger.info("Written $lineNo records to HBase topic db.$topic from '$fileName'.")
                                            }
                                            catch (e: Exception) {
                                                logger.error("Error processing batch on record $lineNo from '$fileName': '${e.message}'.")
                                            }
                                            finally {
                                                batch = mutableListOf()
                                            }
                                        }
                                    }
                                    if (runMode != RUN_MODE_IMPORT) {
                                        val manifestRecord = ManifestRecord(id!!, lastModifiedTimestampLong, database, collection, "IMPORT", "HDI")
                                        writer.write(manifestWriter.csv(manifestRecord))
                                    }
                                } catch (e: Exception) {
                                    logger.error("Error processing record $lineNo from '$fileName': '${e.message}'.")
                                }
                            }
                            succeeded = true
                        } catch (e: Exception) {
                            try {
                                logger.warn("Error on attempt $attempts streaming '$fileName': '${e.message}'.")
                                inputStream.close()
                            }
                            catch (e: Exception) {
                                logger.warn("Failed to close stream: '${e.message}'.")
                            }
                            inputStream = s3.getObject(s3bucket, fileName).objectContent
                        }
                    }
                }

                if (runMode != RUN_MODE_MANIFEST) {
                    if (batch.size > 0) {
                        try {
                            hbase.putBatch(batch)
                            logger.info("Written $lineNo records to HBase topic db.$database.$collection from '$fileName'.")
                            batch = mutableListOf()
                        }
                        catch (e: Exception) {
                            val batchSize = batch.size
                            logger.error("Failed to write batch of size $batchSize to HBase topic db.$database.$collection after processing $lineNo records from '$fileName': '${e.message}'.")
                        }
                    }
                }

                if (runMode != RUN_MODE_IMPORT) {
                    manifestWriter.sendManifest(s3, File(manifestOutputFile), manifestBucket, manifestPrefix)
                }

                logger.info("Processed $lineNo records from the file $fileName")
            }
        }
    }

    fun idObject(json: JsonObject): JsonObject? {
        val id = json.get("_id")

        if (id.isJsonObject) {
            return id.asJsonObject
        }
        else if (id.isJsonPrimitive) {
            val value = id.asJsonPrimitive
            val asObject = JsonObject()
            asObject.addProperty("id", value.asString)
            return asObject
        }
        else {
            return null
        }
    }

    fun getBufferedReader(inputStream: InputStream): BufferedReader {
        return BufferedReader(InputStreamReader(inputStream))
    }

    fun encryptDbObject(dataKeyResult: DataKeyResult, line: String, fileName: String, id: String?) = cipherService.encrypt(dataKeyResult.plaintextDataKey, line.toByteArray())

    fun getDataKey(fileName: String) = keyService.batchDataKey()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }
}
