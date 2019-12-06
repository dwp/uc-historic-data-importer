package app.batch

import app.domain.ManifestRecord
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.text.StringEscapeUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.BufferedInputStream
import java.io.File
import java.io.FileInputStream

class StreamingManifestWriter {


    fun sendManifest(s3: AmazonS3, manifestPath: String, manifestBucket: String, manifestPrefix: String) {
        val manifestFile = File(manifestPath)
        val manifestSize = manifestFile.length()
        val manifestFileName = manifestFile.name
        val manifestFileMetadata = manifestMetadata(manifestFileName, manifestSize)
        val prefix = "$manifestPrefix/$manifestFileName"
        HBaseWriter.logger.info("Writing manifest '$manifestFile' to 's3://$manifestBucket/$manifestPrefix/$manifestFileName', size: $manifestSize")
        BufferedInputStream(FileInputStream(manifestFile)).use { inputStream ->
            val request = PutObjectRequest(manifestBucket, prefix, inputStream, manifestFileMetadata)
            s3.putObject(request)
        }
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


}