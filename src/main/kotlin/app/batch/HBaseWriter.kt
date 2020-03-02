package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.HBaseRecord
import app.domain.ManifestRecord
import app.services.CipherService
import app.services.KeyService
import app.utils.logging.JsonLoggerWrapper
import com.amazonaws.services.s3.AmazonS3
import com.google.gson.Gson
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
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

    @Value("\${s3.manifest.retry.max.attempts:10}")
    private lateinit var maxManifestAttempts: String

    @Value("\${hbase.retry.max.attempts:5}")
    private lateinit var maxAttempts: String // = 5

    @Value("\${hbase.retry.initial.backoff:10000}")
    private lateinit var initialBackoffMillis: String // = 1000L

    @Value("\${hbase.retry.backoff.multiplier:2}")
    private lateinit var backoffMultiplier: String // = 2

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
        logger.info("System stats", "available_processors", "$cpus", "run_mode", runMode)

        var processedFiles = 0
        var processedRecords = 0

        items.forEach { input ->
            logger.info("Processing file", "s3_location", input.fileName)
            val fileName = input.fileName
            val matchResult = filenameRegex.find(fileName)
            if (matchResult != null) {
                var batch = mutableListOf<HBaseRecord>()
                var batchSizeBytes = 0
                val maxBatchVolume = maxBatchSizeBytes.toInt()
                val groups = matchResult.groups
                val database = groups[1]!!.value // can assert nun-null as it matched on the regex
                val collection = groups[2]!!.value // ditto
                val fileNumber = groups[3]!!.value // ditto
                val tableName = "$database:$collection".replace("-", "_")
                ensureTable(tableName)

                val dataKeyResult: DataKeyResult = getDataKey(fileName)
                var fileProcessedRecords = 0
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
                            batchSizeBytes = 0
                            val reader = getBufferedReader(inputStream)
                            reader.forEachLine { line ->
                                if (attempts > 1 && reader.lineNumber < fileProcessedRecords) {
                                    return@forEachLine
                                }
                                try {
                                    val json = messageUtils.parseGson(line)
                                    val id = gson.toJson(idObject(json))
                                    if (StringUtils.isBlank(id) || id == "null") {
                                        logger.warn("Skipping record with missing id ", "line_number", "${reader.lineNumber}", "file_name", fileName)
                                        return@forEachLine
                                    }
                                    val encryptionResult = encryptDbObject(dataKeyResult, line, fileName, id)
                                    val message = messageProducer.produceMessage(json, id, encryptionResult, dataKeyResult,
                                        database, collection)
                                    val messageJsonObject = messageUtils.parseJson(message)
                                    val lastModifiedTimestampStr = messageUtils.getLastModifiedTimestamp(messageJsonObject)
                                    val lastModifiedTimestampLong = messageUtils.getTimestampAsLong(lastModifiedTimestampStr)
                                    val formattedKey = messageUtils.generateKeyFromRecordBody(messageJsonObject)

                                    if (runMode != RUN_MODE_MANIFEST) {
                                        if (batchSizeBytes + message.length >= maxBatchVolume && batch.size > 0) {
                                            try {
                                                logger.info("Attempting to write batch", "batch_records", "${batch.size}", "batch_bytes", "$batchSizeBytes", "topic_name", "db.$database.$collection", "file_name", fileName)
                                                putBatch(tableName, batch)
                                                logger.info("Written batch", "batch_records", "${batch.size}", "batch_bytes", "$batchSizeBytes", "topic_name", "db.$database.$collection", "file_name", fileName)
                                            }
                                            catch (e: Exception) {
                                                logger.error("Failed to write batch", "batch_records", "${batch.size}", "batch_bytes", "$batchSizeBytes", "topic_name", "db.$database.$collection", "file_name", fileName)
                                            }
                                            finally {
                                                batch = mutableListOf()
                                                batchSizeBytes = 0
                                                fileProcessedRecords = reader.lineNumber
                                            }
                                        }
                                        batch.add(HBaseRecord(formattedKey, message.toByteArray(), lastModifiedTimestampLong))
                                        batchSizeBytes += message.length
                                    }
                                    if (runMode != RUN_MODE_IMPORT) {
                                        val manifestRecord = ManifestRecord(id!!, lastModifiedTimestampLong, database, collection, "IMPORT", "HDI")
                                        writer.write(manifestWriter.csv(manifestRecord))
                                    }
                                }
                                catch (e: Exception) {
                                    logger.error("Error processing record", e, "line_number", "${reader.lineNumber}", "file_name", fileName, "error_message", "${e.message}")
                                }
                            }

                            succeeded = true

                            fileProcessedRecords = reader.lineNumber
                            processedRecords += fileProcessedRecords
                            processedFiles += 1
                        }
                        catch (e: Exception) {
                            try {
                                logger.warn("Error streaming file", "attempt_number", "$attempts", "file_name", fileName, "error_message", "${e.message}")
                                inputStream.close()
                            }
                            catch (e: Exception) {
                                logger.error("Failed to stream file", "batch_records", "${batch.size}", "batch_bytes", "$batchSizeBytes", "topic_name", "db.$database.$collection", "file_name", fileName)
                            }

                            inputStream = cipherService.decompressingDecryptingStream(s3.getObject(s3bucket, fileName).objectContent, input.key, input.iv)
                            batch = mutableListOf()
                            batchSizeBytes = 0
                        }
                    }
                }

                if (runMode != RUN_MODE_MANIFEST) {
                    // Put any left-over records into a final undersize batch
                    if (batch.size > 0) {
                        try {
                            logger.info("Attempting to write batch", "batch_records", "${batch.size}", "batch_bytes", "$batchSizeBytes", "topic_name", "db.$database.$collection", "file_name", fileName)
                            putBatch(tableName, batch)
                            logger.info("Written batch", "batch_records", "${batch.size}", "batch_bytes", "$batchSizeBytes", "topic_name", "db.$database.$collection", "file_name", fileName)
                            batch = mutableListOf()
                            batchSizeBytes = 0
                        }
                        catch (e: Exception) {
                            logger.error("Failed to write batch of ${batch.size} records to hbase with topic 'db.$database.$collection' from '$fileName': '${e.message}'.")
                        }
                    }
                }

                if (runMode != RUN_MODE_IMPORT) {
                    manifestWriter.sendManifest(s3, File(manifestOutputFile), manifestBucket, manifestPrefix, maxManifestAttempts.toInt())
                }

                logger.info("Processed records in file", "records_processed", "$fileProcessedRecords", "file_name", fileName)
            }
        }

        logger.info("Processed records and files", "records_processed", "$processedRecords", "files_processed", "$processedFiles")

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

    fun putBatch(table: String, records: List<HBaseRecord>) {

        var success = false
        var attempts = 0
        var exception: Exception? = null
        while (!success && attempts < maxAttempts.toInt()) {
            try {
                hbase.putBatch(table, records)
                success = true
            }
            catch (e: Exception) {
                val delay = if (attempts == 0) initialBackoffMillis.toLong()
                else (initialBackoffMillis.toLong() * attempts * backoffMultiplier.toFloat()).toLong()
                logger.warn("Failed to put batch", "attempt_number", "${attempts + 1}", "max_attempts", maxAttempts, "retry_delay", "$delay", "error_message", "${e.message}")
                Thread.sleep(delay)
                exception = e
            }
            finally {
                attempts++
            }
        }

        if (!success) {
            if (exception != null) {
                throw exception
            }
        }

    }

    fun ensureTable(tableName: String) {
        hbase.ensureTable(tableName)
    }

    fun getBufferedReader(inputStream: InputStream?) = LineNumberReader(InputStreamReader(inputStream))

    fun encryptDbObject(dataKeyResult: DataKeyResult, line: String, fileName: String, id: String?) = cipherService.encrypt(dataKeyResult.plaintextDataKey, line.toByteArray())

    fun getDataKey(fileName: String) = keyService.batchDataKey()

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(HBaseWriter::class.toString())
    }
}
