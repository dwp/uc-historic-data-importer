package app.batch

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

@Component
class S3Reader(private val s3client: AmazonS3) : ItemReader<EncryptedStream> {

    @Autowired
    private lateinit var s3Client: AmazonS3
    @Autowired
    private lateinit var keyPairGenerator: KeyPairGenerator
    private var iterator: ListIterator<S3ObjectSummaryPair>? = null

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String

    @Value("\${s3.prefix.folder}")
    private lateinit var s3PrefixFolder: String

    @Value("\${s3.key.regex:([A-Za-z]*\\.[A-Za-z]*\\.[0-9]{4}\\.json\\.gz)}")
    private lateinit var s3KeyRegex: String

    @Value("\${s3.data.key.extension:\\.enc\$}")
    private lateinit var s3DataKeyExtension: String

    @Value("\${s3.metadata.key.extension:\\.encryption\\.json\$}")
    private lateinit var s3MetadataKeyExtension: String

    override fun read(): EncryptedStream? {
        val iterator = getS3ObjectSummariesIterator(s3Client, s3BucketName)
        return if (iterator.hasNext()) {
            iterator.next().let {
                val dataInputStream = it.data?.let { it1 -> getS3ObjectInputStream(it1, s3Client, s3BucketName) }
                val metadataInputStream = it.metadata?.let { it1 -> getS3ObjectInputStream(it1, s3Client, s3BucketName) }
                logger.info(printInputStream(dataInputStream))
                logger.info(printInputStream(metadataInputStream))
                return EncryptedStream(dataInputStream, metadataInputStream)
            }
        } else {
            null
        }
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

    private fun printInputStream(inputStream : S3ObjectInputStream?) : String{
        val result = ByteArrayOutputStream()
        val buffer = ByteArray(1024)
        var length = 0
        while (length  != -1) {
            result.write(buffer, 0, length)
            length = inputStream?.read(buffer)!!
        }

        return result.toString(StandardCharsets.UTF_8.name())
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())

    }
}
