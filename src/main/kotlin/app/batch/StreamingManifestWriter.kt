package app.batch

import app.domain.ManifestRecord
import app.utils.logging.JsonLoggerWrapper
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.text.StringEscapeUtils
import java.io.File
import java.io.FileInputStream

open class StreamingManifestWriter {

    fun sendManifest(s3: AmazonS3, manifestFile: File, manifestBucket: String, manifestPrefix: String, maxManifestAttempts: Int) {
        var attempts = 0
        var success = false
        while (!success && attempts < maxManifestAttempts) {
            try {
                val manifestSize = manifestFile.length()
                val manifestFileName = manifestFile.name
                if (manifestSize > 0) {
                    val manifestFileMetadata = manifestMetadata(manifestFileName, manifestSize)
                    val prefix = "$manifestPrefix/$manifestFileName"

                    FileInputStream(manifestFile).use { inputStream ->
                        val request = PutObjectRequest(manifestBucket, prefix, inputStream, manifestFileMetadata)
                        s3.putObject(request)
                        logger.info("Written manifest", "attempt_number", "${attempts + 1}", "manifest_size", "$manifestSize", "s3_location", "s3://$manifestBucket/$manifestPrefix/$manifestFileName")
                        success = true
                        val deleted = manifestFile.delete()
                        logger.info("Deleted manifest", "succeeded", "${deleted}", "manifest_file", "$manifestFile")
                        return
                    }
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

    fun manifestMetadata(fileName: String, size: Long) =
            ObjectMetadata().apply {
                contentType = "text/plain"
                addUserMetadata("x-amz-meta-title", fileName)
                contentLength = size
            }

    fun csv(manifestRecord: ManifestRecord) =
        "${escape(manifestRecord.id)}|${escape(manifestRecord.timestamp.toString())}|${escape(manifestRecord.db)}|${escape(manifestRecord.collection)}|${escape(manifestRecord.source)}|${escape(manifestRecord.outerType)}|${escape(manifestRecord.originalId)}|${escape(manifestRecord.innerType)}\n"

    fun topicName(db: String, collection: String) = "db.$db.$collection"

    private fun escape(value: String) = StringEscapeUtils.escapeCsv(value)

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(StreamingManifestWriter::class.toString())
    }

}
