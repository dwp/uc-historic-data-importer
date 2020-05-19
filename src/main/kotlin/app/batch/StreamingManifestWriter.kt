package app.batch

import app.domain.ManifestRecord
import app.utils.logging.JsonLoggerWrapper
import com.amazonaws.services.s3.AmazonS3
import org.apache.commons.text.StringEscapeUtils
import java.io.File

open class StreamingManifestWriter {

    fun sendManifest(s3: AmazonS3, manifestFile: File, manifestBucket: String, manifestPrefix: String, maxManifestAttempts: Int) {
        var attempts = 0
        var success = false
        while (!success && attempts < maxManifestAttempts) {
            try {
                val manifestSize = manifestFile.length()
                val manifestFileName = manifestFile.name
                if (manifestSize > 0) {
                    val prefix = "$manifestPrefix/$manifestFileName"

                    s3.putObject(manifestBucket, prefix, manifestFile)
                    logger.info("Written manifest",
                            "attempt_number", "${attempts + 1}",
                            "manifest_file", "$manifestFile",
                            "manifest_size", "$manifestSize", "s3_location", "s3://$manifestBucket/$manifestPrefix/$manifestFileName")
                    val deleted = manifestFile.delete()
                    logger.info("Deleted manifest", "succeeded", "${deleted}", "manifest_file", "$manifestFile")
                    success = true
                    return
                }
                else {
                    logger.info("Skipped zero-byte manifest", "manifest_size", "$manifestSize", "manifest_file_name", manifestFileName)
                    success = true
                    return

                }
            }
            catch (e: Exception) {
                ++attempts
                logger.warn("Failed to write manifest", "attempt_number", "$attempts", "max_attempts", "$maxManifestAttempts", "error_message", "${e.message}")
            }
        }

        logger.error("Failed to write manifest after max attempts - giving up", "manifest_file", manifestFile.name, "max_attempts", "$maxManifestAttempts")
    }

    fun csv(manifestRecord: ManifestRecord) =
        "${escape(manifestRecord.id)}|${escape(manifestRecord.timestamp.toString())}|${escape(manifestRecord.db)}|${escape(manifestRecord.collection)}|${escape(manifestRecord.source)}|${escape(manifestRecord.outerType)}|${escape(manifestRecord.originalId)}|${escape(manifestRecord.innerType)}\n"

    fun topicName(db: String, collection: String) = "db.$db.$collection"

    private fun escape(value: String) = StringEscapeUtils.escapeCsv(value)

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(StreamingManifestWriter::class.toString())
    }

}
