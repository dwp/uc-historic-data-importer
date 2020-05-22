package app.batch

import app.domain.*
import app.services.CipherService
import app.services.KeyService
import app.utils.logging.JsonLoggerWrapper
import com.amazonaws.services.s3.AmazonS3
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.*


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
                val collection = coalesced(groups[2]!!.value)
                val fileNumber = groups[3]!!.value // ditto
                val tableName = "$database:$collection".replace("-", "_")
                ensureTable(tableName)

                val dataKeyResult: DataKeyResult = getDataKey(fileName)
                var fileProcessedRecords = 0
                val gson = GsonBuilder().serializeNulls().create()
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
                            reader.forEachLine { lineFromDump ->
                                if (attempts > 1 && reader.lineNumber < fileProcessedRecords) {
                                    return@forEachLine
                                }
                                try {
                                    val (lineAsJsonBeforeReFormatting, recordIsRemovedRecord) = reformatRemoved(lineFromDump)
                                    val (lineAsJson, recordIsArchivedRecord) = reformatArchived(lineAsJsonBeforeReFormatting)

                                    val originalId = lineAsJson.get("_id")

                                    val (id, idModificationType) = normalisedId(gson, originalId)
                                    if (StringUtils.isBlank(id) || id == "null") {
                                        logger.warn("Skipping record with missing id ", "line_number", "${reader.lineNumber}", "file_name", fileName)
                                        return@forEachLine
                                    }

                                    val (createdDateTime, createdDateTimeWasModified) = optionalDateTime(gson, CREATED_DATE_TIME_FIELD, lineAsJson)
                                    val (removedDateTime, removedDateTimeWasModified) = optionalDateTime(gson, REMOVED_DATE_TIME_FIELD, lineAsJson)
                                    val (archivedDateTime, archivedDateTimeWasModified) = optionalDateTime(gson, ARCHIVED_DATE_TIME_FIELD, lineAsJson)

                                    val originalLastModifiedDateTime = lineAsJson.get(LAST_MODIFIED_DATE_TIME_FIELD)
                                    val (lastModifiedDateTime, lastModifiedDateTimeSourceField)
                                            = lastModifiedDateTime(gson, originalLastModifiedDateTime, createdDateTime)

                                    var updatedLineAsJson = lineAsJson
                                    if (idModificationType == IdModification.FlattenedMongoId) {
                                        updatedLineAsJson = overwriteFieldValue(gson, "_id", id, updatedLineAsJson)
                                    }
                                    else if (idModificationType == IdModification.FlattenedInnerDate) {
                                        updatedLineAsJson = overwriteFieldValueWithObject(gson, "_id", gson.fromJson(id, JsonObject::class.java), updatedLineAsJson)
                                    }

                                    if (lastModifiedDateTimeSourceField != LAST_MODIFIED_DATE_TIME_FIELD) {
                                        updatedLineAsJson = overwriteFieldValue(gson, LAST_MODIFIED_DATE_TIME_FIELD, lastModifiedDateTime, updatedLineAsJson)
                                    }

                                    if (createdDateTimeWasModified) {
                                        updatedLineAsJson = overwriteFieldValue(gson, CREATED_DATE_TIME_FIELD, createdDateTime, updatedLineAsJson)
                                    }

                                    if (removedDateTimeWasModified) {
                                        updatedLineAsJson = overwriteFieldValue(gson, REMOVED_DATE_TIME_FIELD, removedDateTime, updatedLineAsJson)
                                    }

                                    if (archivedDateTimeWasModified) {
                                        updatedLineAsJson = overwriteFieldValue(gson, ARCHIVED_DATE_TIME_FIELD, archivedDateTime, updatedLineAsJson)
                                    }

                                    val encryptionResult = encryptDbObject(dataKeyResult.plaintextDataKey, gson.toJson(updatedLineAsJson))
                                    val idWasModified = (idModificationType == IdModification.FlattenedMongoId  ||
                                            idModificationType == IdModification.FlattenedInnerDate)

                                    val idIsString = (idModificationType == IdModification.UnmodifiedStringId) ||
                                            (idModificationType == IdModification.FlattenedMongoId)

                                    val messageWrapper = messageProducer.produceMessage(updatedLineAsJson, id,
                                            idIsString, 
                                            idWasModified,
                                            lastModifiedDateTime,
                                            lastModifiedDateTimeSourceField,
                                            StringUtils.isNotBlank(createdDateTime) && createdDateTimeWasModified,
                                            StringUtils.isNotBlank(removedDateTime) && removedDateTimeWasModified,
                                            StringUtils.isNotBlank(archivedDateTime) && archivedDateTimeWasModified,
                                            recordIsRemovedRecord,
                                            recordIsArchivedRecord,
                                            encryptionResult,
                                            dataKeyResult,
                                            database,
                                            collection)

                                    val messageJsonObject = messageUtils.parseJson(messageWrapper)
                                    val lastModifiedTimestampLong = messageUtils.getTimestampAsLong(lastModifiedDateTime)
                                    val formattedKey = messageUtils.generateKeyFromRecordBody(messageJsonObject)
                                    if (runMode != RUN_MODE_MANIFEST) {
                                        if (batchSizeBytes + messageWrapper.length >= maxBatchVolume && batch.size > 0) {
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
                                        batch.add(HBaseRecord(formattedKey, messageWrapper.toByteArray(), lastModifiedTimestampLong))
                                        batchSizeBytes += messageWrapper.length
                                    }
                                    if (runMode != RUN_MODE_IMPORT) {
                                        val idForManifest = if (idIsString) id else messageUtils.sortJsonStringByKey(id)
                                        val incomingId = if (idWasModified) incomingId(gson, originalId) else idForManifest
                                        val outerType = messageJsonObject["@type"]?.toString() ?: "TYPE_NOT_SET"
                                        val innerType = messageUtils.getType(messageJsonObject)

                                        val timestampForManifest =
                                                manifestTimestamp(innerType, lastModifiedTimestampLong,
                                                        removedDateTime = removedDateTime,
                                                        archivedDateTime = archivedDateTime,
                                                        createdDateTime = createdDateTime)

                                        val manifestRecord = ManifestRecord(idForManifest, timestampForManifest,
                                                database, collection, "IMPORT", outerType, innerType, incomingId)
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

    fun coalesced(collection: String): String {
        val coalescedName = COALESCED_COLLECTION.replace(collection, "")
        if (collection != coalescedName) {
            logger.info("Using coalesced collection", "original_name", collection, "coalesced_name", coalescedName)
        }
        return coalescedName
    }

    fun manifestTimestamp(innerType: String, lastModifiedTimestampLong: Long,
                          removedDateTime: String,
                          archivedDateTime: String,
                          createdDateTime: String) = try {
        when (innerType) {
            MONGO_DELETE -> {
                if (StringUtils.isNotBlank(removedDateTime)) {
                    messageUtils.getTimestampAsLong(removedDateTime)
                }
                else if (StringUtils.isNotBlank(archivedDateTime)) {
                    messageUtils.getTimestampAsLong(archivedDateTime)
                }
                else {
                    lastModifiedTimestampLong
                }
            }
            MONGO_INSERT -> {
                if (StringUtils.isNotBlank(createdDateTime)) {
                    messageUtils.getTimestampAsLong(createdDateTime)
                }
                else {
                    lastModifiedTimestampLong
                }
            }
            else -> {
                lastModifiedTimestampLong
            }
        }
        }
        catch (e: ParseException) {
            lastModifiedTimestampLong
        }

    fun reformatRemoved(recordFromDump: String): Pair<JsonObject, Boolean> {
        val record = messageUtils.parseGson(recordFromDump)

        return if (record.has(REMOVED_RECORD_FIELD)) {
            val removedRecord = record.getAsJsonObject(REMOVED_RECORD_FIELD).deepCopy()
            copyField(LAST_MODIFIED_DATE_TIME_FIELD, record, removedRecord)
            copyField(REMOVED_DATE_TIME_FIELD, record, removedRecord)
            copyField(TIMESTAMP_FIELD, record, removedRecord)
            removedRecord.addProperty("@type", MONGO_DELETE)
            Pair(removedRecord.deepCopy(), true)
        } else {
            Pair(record, false)
        }
    }

    fun reformatArchived(record: JsonObject): Pair<JsonObject, Boolean> {
        return if (record.has(ARCHIVED_RECORD_FIELD)) {
            val archivedRecord = record.getAsJsonObject(ARCHIVED_RECORD_FIELD).deepCopy()
            copyField(LAST_MODIFIED_DATE_TIME_FIELD, record, archivedRecord)
            copyField(ARCHIVED_DATE_TIME_FIELD, record, archivedRecord)
            copyField(TIMESTAMP_FIELD, record, archivedRecord)
            archivedRecord.addProperty("@type", MONGO_DELETE)
            Pair(archivedRecord.deepCopy(), true)
        } else {
            Pair(record, false)
        }
    }

    fun copyField(fieldName: String, sourceRecord: JsonObject, targetRecord: JsonObject) {
        if (sourceRecord.has(fieldName)) {
            if (targetRecord.has(fieldName)) {
                targetRecord.remove(fieldName)
            }
            targetRecord.add(fieldName, sourceRecord.get(fieldName))
        }
    }

    fun overwriteFieldValue(gson: Gson, fieldKey: String, fieldValue: String, json: JsonObject): JsonObject {
        json.remove(fieldKey)
        json.addProperty(fieldKey, fieldValue)
        return json
    }

    fun overwriteFieldValueWithObject(gson: Gson, fieldKey: String, fieldValue: JsonElement, json: JsonObject): JsonObject {
        json.remove(fieldKey)
        json.add(fieldKey, fieldValue)
        return json
    }

    fun normalisedId(gson: Gson, id: JsonElement?): Pair<String, IdModification> {
        if (id != null) {
            return if (id.isJsonObject) {
                val obj = id.asJsonObject!!.deepCopy()
                if (obj.size() == 1 && obj["\$oid"] != null && obj["\$oid"].isJsonPrimitive) {
                    Pair(obj["\$oid"].asJsonPrimitive.asString, IdModification.FlattenedMongoId)
                }
                else if (hasKnownDateField(obj)) {
                    var flattened = flattenedDateField(obj, CREATED_DATE_TIME_FIELD)
                    flattened = flattenedDateField(flattened, LAST_MODIFIED_DATE_TIME_FIELD)
                    flattened = flattenedDateField(flattened, REMOVED_DATE_TIME_FIELD)
                    flattened = flattenedDateField(flattened, ARCHIVED_DATE_TIME_FIELD)
                    Pair(gson.toJson(flattened), IdModification.FlattenedInnerDate)
                }
                else {
                    Pair(gson.toJson(id.asJsonObject), IdModification.UnmodifiedObjectId)
                }
            }
            else if (id.isJsonPrimitive) {
                Pair(id.asJsonPrimitive.asString, IdModification.UnmodifiedStringId)
            }
            else {
                Pair("", IdModification.InvalidId)
            }
        }
        else {
            return Pair("", IdModification.InvalidId)
        }
    }

    private fun hasKnownDateField(obj: JsonObject) = hasDateField(obj, CREATED_DATE_TIME_FIELD) ||
                hasDateField(obj, LAST_MODIFIED_DATE_TIME_FIELD) ||
                hasDateField(obj, REMOVED_DATE_TIME_FIELD) ||
                hasDateField(obj, ARCHIVED_DATE_TIME_FIELD)

    private fun flattenedDateField(obj: JsonObject, dateField: String): JsonObject {
        if (hasDateField(obj, dateField)) {
            val dateString = obj[dateField].asJsonObject["\$date"].asString
            obj.remove(dateField)
            obj.addProperty(dateField, kafkaDateFormat(dateString))
        }

        return obj
    }

    fun hasDateField(obj: JsonObject, dateField: String) =
            obj[dateField] != null &&
            obj[dateField].isJsonObject &&
            obj[dateField].asJsonObject.size() == 1 &&
            obj[dateField].asJsonObject["\$date"] != null &&
            obj[dateField].asJsonObject["\$date"].isJsonPrimitive

    fun lastModifiedDateTime(gson: Gson, incomingDateTime: JsonElement?, createdDateTime: String): Pair<String, String> {

        val fallBackDate = if (StringUtils.isNotBlank(createdDateTime)) createdDateTime else EPOCH
        val fallBackField = if (fallBackDate == EPOCH) EPOCH_FIELD else CREATED_DATE_TIME_FIELD

        if (incomingDateTime != null) {
            when {
                incomingDateTime.isJsonObject -> {
                    val obj = incomingDateTime.asJsonObject!!
                    return if (obj.size() == 1 && obj["\$date"] != null && obj["\$date"].isJsonPrimitive) {
                        Pair(kafkaDateFormat(obj["\$date"].asJsonPrimitive.asString), LAST_MODIFIED_DATE_TIME_FIELD_STRIPPED)
                    }
                    else {
                        logger.debug("_lastModifiedDateTime was an object, without a \$date field", "incoming_value", "$incomingDateTime", "outgoing_value", fallBackDate)
                        Pair(fallBackDate, fallBackField)
                    }
                }
                incomingDateTime.isJsonPrimitive -> {
                    val outgoingValue = incomingDateTime.asJsonPrimitive.asString
                    logger.debug("$LAST_MODIFIED_DATE_TIME_FIELD was a string", "incoming_value", "$incomingDateTime", "outgoing_value", outgoingValue)
                    return Pair(outgoingValue, LAST_MODIFIED_DATE_TIME_FIELD)
                }
                else -> {
                    logger.debug("Invalid $LAST_MODIFIED_DATE_TIME_FIELD object", "incoming_value", "$incomingDateTime", "outgoing_value", fallBackDate)
                    return Pair(fallBackDate, fallBackField)
                }
            }
        }
        else {
            logger.debug("No incoming $LAST_MODIFIED_DATE_TIME_FIELD object", "incoming_value", "$incomingDateTime", "outgoing_value", fallBackDate)
            return Pair(fallBackDate, fallBackField)
        }
    }

    fun optionalDateTime(gson: Gson, name: String, parent: JsonObject): Pair<String, Boolean> {
        val incomingDateTime = parent.get(name)
        if (incomingDateTime != null) {
            when {
                incomingDateTime.isJsonObject -> {
                    val obj = incomingDateTime.asJsonObject!!
                    return if (obj.size() == 1 && obj["\$date"] != null && obj["\$date"].isJsonPrimitive) {
                        Pair(kafkaDateFormat(obj["\$date"].asJsonPrimitive.asString), true)
                    }
                    else {
                        logger.debug("$name was an object, without a \$date field", "incoming_value", "$incomingDateTime", "outgoing_value", "")
                        Pair("", true)
                    }
                }
                incomingDateTime.isJsonPrimitive -> {
                    val outgoingValue = incomingDateTime.asJsonPrimitive.asString
                    logger.debug("$name was a string", "incoming_value", "$incomingDateTime", "outgoing_value", outgoingValue)
                    return Pair(outgoingValue, false)
                }
                else -> {
                    logger.warn("Invalid $name object", "incoming_value", "$incomingDateTime", "outgoing_value", "")
                    return Pair("", true)
                }
            }
        }
        else {
            logger.debug("No incoming $name object", "incoming_value", "null", "outgoing_value", "")
            return Pair("", false)
        }
    }

    fun incomingId(gson: Gson, id: JsonElement?): String {
        return if (id != null) {
            when {
                id.isJsonObject -> {
                    messageUtils.sortJsonStringByKey(id.asJsonObject.toString())
                }
                id.isJsonPrimitive -> {
                    id.asJsonPrimitive.asString
                }
                else -> {
                    ""
                }
            }
        }
        else {
            ""
        }
    }

    fun kafkaDateFormat(input: String): String {
        val parsedDateTime = getValidParsedDateTime(input)
        val df = SimpleDateFormat(VALID_OUTGOING_DATE_FORMAT)
        df.timeZone = TimeZone.getTimeZone("UTC")
        return df.format(parsedDateTime)
    }

    fun getValidParsedDateTime(timestampAsString: String): Date {
        VALID_DATE_FORMATS.forEach {
            try {
                val df = SimpleDateFormat(it)
                df.timeZone = TimeZone.getTimeZone("UTC")
                return df.parse(timestampAsString)
            } catch (e: Exception) {
                logger.debug("timestampAsString did not match valid formats", "date_time_string", timestampAsString, "failed_format", it)
            }
        }
        throw Exception("Unparseable date found: '$timestampAsString', did not match any supported date formats")
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

    fun encryptDbObject(dataKey: String, line: String): EncryptionResult {
        return cipherService.encrypt(dataKey, line.toByteArray())
    }

    fun getDataKey(fileName: String) = keyService.batchDataKey()

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(HBaseWriter::class.toString())
        const val LAST_MODIFIED_DATE_TIME_FIELD = "_lastModifiedDateTime"
        const val CREATED_DATE_TIME_FIELD = "createdDateTime"
        const val REMOVED_DATE_TIME_FIELD = "_removedDateTime"
        const val ARCHIVED_DATE_TIME_FIELD = "_archivedDateTime"
        const val EPOCH = "1980-01-01T00:00:00.000+0000"

        const val MONGO_DELETE = "MONGO_DELETE"
        const val MONGO_INSERT = "MONGO_INSERT"

        const val VALID_INCOMING_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        const val VALID_OUTGOING_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"
        val VALID_DATE_FORMATS = listOf(VALID_INCOMING_DATE_FORMAT, VALID_OUTGOING_DATE_FORMAT)
        val COALESCED_COLLECTION = Regex("-(archived|eight|eighteen|eleven|fifteen|five|four|fourteen|nine|nineteen|one|seven|seventeen|six|sixteen|ten|thirteen|thirty|thirtyone|thirtytwo|three|twelve|twenty|twentyeight|twentyfive|twentyfour|twentynine|twentyone|twentyseven|twentysix|twentythree|twentytwo|two)$")

        private const val LAST_MODIFIED_DATE_TIME_FIELD_STRIPPED = "_lastModifiedDateTimeStripped"
        private const val EPOCH_FIELD = "epoch"
        private const val RUN_MODE_MANIFEST = "manifest"
        private const val RUN_MODE_IMPORT = "import"
        private const val REMOVED_RECORD_FIELD = "_removed"
        private const val ARCHIVED_RECORD_FIELD = "_archived"
        private const val TIMESTAMP_FIELD = "timestamp"

        enum class IdModification {
            UnmodifiedObjectId,
            UnmodifiedStringId,
            FlattenedMongoId,
            FlattenedInnerDate,
            InvalidId
        }
    }
}
