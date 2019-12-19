package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.HBaseRecord
import app.domain.ManifestRecord
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
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*

@Component
@Profile("hbaseWriter")
class HBaseWriter : ItemWriter<DecompressedStream> {

    @Autowired
    private lateinit var s3: AmazonS3

    @Autowired
    private lateinit var keyService: KeyService

    @Autowired
    private lateinit var hbase: HbaseClient

    @Autowired
    private lateinit var messageProducer: MessageProducer

    @Autowired
    private lateinit var messageUtils: MessageUtils

    @Autowired
    private lateinit var cipherService: CipherService

    @Value("\${hbase.retry.max.attempts:5}")
    private lateinit var maxAttempts: String // = 5

    @Value("\${hbase.retry.initial.backoff:10000}")
    private lateinit var initialBackoffMillis: String // = 1000L

    @Value("\${hbase.retry.backoff.multiplier:2}")
    private lateinit var backoffMultiplier: String // = 2

    @Value("\${kafka.topic.prefix:db}")
    private lateinit var kafkaTopicPrefix: String

    @Value("\${manifest.output.directory:.}")
    private lateinit var manifestOutputDirectory: String


    @Value("\${max.batch.size.bytes:100000000}")
    private lateinit var maxBatchSizeBytes: String // max size of a batch in bytes

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
                val maxBatchVolume = maxBatchSizeBytes.toInt()
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
                            var batchSizeBytes = 0
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
                                        batchSizeBytes += message.length
                                        if (batchSizeBytes >= maxBatchVolume) {
                                            try {
                                                putBatch(batch)
                                                logger.info("Written ${batch.size} records to HBase topic $topic from '$fileName', batchSizeBytes: '$batchSizeBytes'.")
                                            }
                                            catch (e: Exception) {
                                                logger.error("Error processing batch on record $lineNo from '$fileName': '${e.message}'.")
                                            }
                                            finally {
                                                batch = mutableListOf()
                                                batchSizeBytes = 0
                                            }
                                        }
                                    }
                                    if (runMode != RUN_MODE_IMPORT) {
                                        val manifestRecord = ManifestRecord(id!!, lastModifiedTimestampLong, database, collection, "IMPORT", "HDI")
                                        writer.write(manifestWriter.csv(manifestRecord))
                                    }
                                }
                                catch (e: Exception) {
                                    logger.error("Error processing record $lineNo from '$fileName': '${e.message}'.", e)
                                }
                            }
                            succeeded = true
                        }
                        catch (e: Exception) {
                            try {
                                logger.warn("Error on attempt $attempts streaming '$fileName': '${e.message}'.")
                                inputStream.close()
                            }
                            catch (e: Exception) {
                                logger.warn("Failed to close stream: '${e.message}'.")
                            }

                            inputStream = cipherService.decompressingDecryptingStream(s3.getObject(s3bucket, fileName).objectContent, input.key, input.iv)
                            lineNo = 0

                        }
                    }
                }

                if (runMode != RUN_MODE_MANIFEST) {
                    if (batch.size > 0) {
                        try {
                            putBatch(batch)
                            logger.info("Written ${batch.size} records to HBase topic db.$database.$collection from '$fileName'.")
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

    fun putBatch(records: List<HBaseRecord>) {

        var success = false
        var attempts = 0
        var exception: Exception? = null
        while (!success && attempts < maxAttempts.toInt()) {
            try {
                hbase.putBatch(records)
                success = true
            }
            catch (e: Exception) {
                val delay = if (attempts == 0) initialBackoffMillis.toLong()
                else (initialBackoffMillis.toLong() * attempts * backoffMultiplier.toFloat()).toLong()
                logger.warn("Failed to put batch on attempt $attempts, will retry in $delay ms, if $attempts still < $maxAttempts.")
                Thread.sleep(delay)
                exception = e
            }
            finally {
                attempts++
            }
        }

        if (!success) {
            if (exception != null) {
                throw exception!!
            }
        }

    }

    fun getBufferedReader(inputStream: InputStream?) = BufferedReader(InputStreamReader(inputStream))

    fun encryptDbObject(dataKeyResult: DataKeyResult, line: String, fileName: String, id: String?) = cipherService.encrypt(dataKeyResult.plaintextDataKey, line.toByteArray())

    fun getDataKey(fileName: String) = keyService.batchDataKey()

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }
}
