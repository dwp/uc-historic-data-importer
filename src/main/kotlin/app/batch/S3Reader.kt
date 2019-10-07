package app.batch

import app.domain.InputStreamPair
import app.domain.S3ObjectSummaryPair
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.charset.StandardCharsets

@Component
class S3Reader(private val s3client: AmazonS3, private val keyPairGenerator: KeyPairGenerator) : ItemReader<InputStreamPair> {

    private var iterator: ListIterator<S3ObjectSummaryPair>? = null

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String

    @Value("\${s3.prefix.folder}")
    private lateinit var s3PrefixFolder: String

    @Value("\${s3.key.regex:([A-Za-z]*\\.[A-Za-z]*\\.[0-9]{4}\\.json\\.gz)}")
    private lateinit var s3KeyRegex: String

    @Value("\${s3.data.key.extension:\\.enc$}")
    private lateinit var s3DataKeyExtension: String

    @Value("\${s3.metadata.key.extension:\\.encryption\\.json$}")
    private lateinit var s3MetadataKeyExtension: String

    override fun read(): InputStreamPair? {
        val iterator = getS3ObjectSummariesIterator(s3client, s3BucketName)
        return if (iterator.hasNext()) {
            iterator.next().let {
                val dataInputStream = it.data?.let { it1 -> getS3ObjectInputStream(it1, s3client, s3BucketName) }
                val metadataInputStream = it.metadata?.let { it1 -> getS3ObjectInputStream(it1, s3client, s3BucketName) }
                return InputStreamPair(dataInputStream!!, metadataInputStream!!, it.data.key)
            }
        }
        else {
            null
        }
    }

    fun reset() {
        iterator = null
    }

    @Synchronized
    private fun getS3ObjectSummariesIterator(s3Client: AmazonS3, bucketName: String): ListIterator<S3ObjectSummaryPair> {
        if (null == iterator) {
            val objectSummaries = s3Client.listObjectsV2(bucketName, s3PrefixFolder).objectSummaries
            val objectSummaryKeyMap = objectSummaries.map { it.key to it }.toMap()
            val keyPairs = keyPairGenerator.generateKeyPairs(objectSummaries.map { it.key }, s3KeyRegex.toRegex(), s3DataKeyExtension.toRegex(), s3MetadataKeyExtension.toRegex())
            val pairs = keyPairs.map {
                val obj = objectSummaryKeyMap[it.dataKey]
                val meta = objectSummaryKeyMap[it.metadataKey]
                S3ObjectSummaryPair(obj, meta)
            }
            iterator = pairs.listIterator()
        }
        return iterator!!
    }

    private fun getS3ObjectInputStream(os: S3ObjectSummary, s3Client: AmazonS3, bucketName: String): S3ObjectInputStream {
        return s3Client.getObject(bucketName, os.key).objectContent
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3Reader::class.toString())
    }
}
