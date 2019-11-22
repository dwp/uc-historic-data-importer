package app.batch

import app.domain.ManifestRecord
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.text.StringEscapeUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

@Component
class ManifestWriter {

    @Synchronized
    fun generateManifest(manifestRecords: List<ManifestRecord>, db: String, collection: String, fileNumber: String) {

        try {
            val topicName = getTopicName(db, collection)
            val manifestFileName = generateManifestFileFormat(topicName, fileNumber.toInt())
            val manifestRecordString = generateEscapedCSV(manifestRecords)
            writeManifest(manifestRecordString, manifestFileName)
        } catch (e: Exception) {
            val joinedIds = manifestRecords.map { it.id }.joinToString(":")
            logger.error("Exception while writing ids: '${joinedIds}' of db: '${manifestRecords[0].db}, collection: ${manifestRecords[0].collection}' to manifest files in S3", e)
        }
    }

    fun writeManifest(manifestContent: String, fileName: String) {

        val byteArrayOutputStream = ByteArrayOutputStream()
        BufferedOutputStream(byteArrayOutputStream).use {
            it.write(manifestContent.toByteArray(StandardCharsets.UTF_8))
        }

        val manifestFileBytes = byteArrayOutputStream.toByteArray()
        val bytesSize = manifestFileBytes.size.toLong()
        logger.info("Writing file 's3://$s3ManifestBucketName/$fileName' of '$bytesSize' bytes.")

        val inputStream = ByteArrayInputStream(manifestFileBytes)
        val bufferedInputStream = BufferedInputStream(inputStream)

        val manifestFileMetadata = generateManifestFileMetadata(fileName, bytesSize)

        val request = PutObjectRequest(s3ManifestBucketName, fileName, bufferedInputStream, manifestFileMetadata)
        s3Client.putObject(request)

    }


    fun generateManifestFileFormat(topicName: String, fileNumber: Int): String {
        return "${s3ManifestPrefixFolder}/$topicName-%06d.csv".format(fileNumber)
    }

    fun generateEscapedCSV(manifestRecords: List<ManifestRecord>): String {
        val manifestData = manifestRecords.map { "${escape(it.id)},${escape(it.timestamp.toString())},${escape(it.db)},${escape(it.collection)},${escape(it.source)}" }
        return manifestData.joinToString("\n")
    }

    fun generateManifestFileMetadata(manifestFileName: String, bytesSize: Long): ObjectMetadata {
        val metadata = ObjectMetadata()
        metadata.contentType = "binary/octetstream"
        metadata.addUserMetadata("x-amz-meta-title", manifestFileName)
        metadata.contentLength = bytesSize
        return metadata
    }

    fun getTopicName(db: String, collection: String): String {
        return "db.$db.$collection"
    }

    private fun escape(value: String): String {
        return StringEscapeUtils.escapeCsv(value)
    }


    companion object {
        val logger: Logger = LoggerFactory.getLogger(ManifestWriter::class.toString())
    }

    @Autowired
    private lateinit var s3Client: AmazonS3

    @Value("\${s3.manifest.prefix.folder}")
    private lateinit var s3ManifestPrefixFolder: String

    @Value("\${s3.manifest.bucket}")
    private lateinit var s3ManifestBucketName: String
}
