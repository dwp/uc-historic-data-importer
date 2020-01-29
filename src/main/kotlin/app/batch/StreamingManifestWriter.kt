package app.batch

import app.domain.ManifestRecord
import app.utils.logging.*
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream

open class StreamingManifestWriter {

    fun sendManifest(s3: AmazonS3, manifestFile: File, manifestBucket: String, manifestPrefix: String, maxManifestAttempts: Int) {
        var attempts = 0
        var success = false
        while (!success && attempts < maxManifestAttempts.toInt()) {
            try {
                val manifestSize = manifestFile.length()
                val manifestFileName = manifestFile.name
                val manifestFileMetadata = manifestMetadata(manifestFileName, manifestSize)
                val prefix = "$manifestPrefix/$manifestFileName"
                BufferedInputStream(FileInputStream(manifestFile)).use { inputStream ->
                    val request = PutObjectRequest(manifestBucket, prefix, inputStream, manifestFileMetadata)
                    s3.putObject(request)
                    logger.info("Written manifest '$manifestFile' on attempt ${attempts + 1}/$maxManifestAttempts to 's3://$manifestBucket/$manifestPrefix/$manifestFileName', size: $manifestSize")
                    success = true
                    return
                }
            } catch (e: Exception) {
                ++attempts
                logger.warn("Failed to write manifest '${manifestFile}' on attempt $attempts/$maxManifestAttempts: '${e.message}'")
            }
        }

        logger.error("Failed to write manifest '${manifestFile}' after $maxManifestAttempts attempts, giving up.")
    }

    fun manifestMetadata(fileName: String, size: Long) =
            ObjectMetadata().apply {
                contentType = "text/plain"
                addUserMetadata("x-amz-meta-title", fileName)
                contentLength = size
            }

    fun csv(manifestRecord: ManifestRecord) =
            "${escape(manifestRecord.id)},${escape(manifestRecord.timestamp.toString())},${escape(manifestRecord.db)},${escape(manifestRecord.collection)},${escape(manifestRecord.source)},${escape(manifestRecord.externalSource)}\n"

    fun topicName(db: String, collection: String) = "db.$db.$collection"

    private fun escape(value: String) =  StringEscapeUtils.escapeCsv(value)

    companion object {
        val logger: Logger = LoggerFactory.getLogger(StreamingManifestWriter::class.toString())
    }

}
