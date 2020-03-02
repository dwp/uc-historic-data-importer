package app.batch

import app.domain.InputStreamPair
import app.domain.S3ObjectSummaryPair
import app.utils.logging.JsonLoggerWrapper
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component


@Component
class S3Reader(private val s3client: AmazonS3, private val keyPairGenerator: KeyPairGenerator) : ItemReader<InputStreamPair> {

    private var iterator: ListIterator<S3ObjectSummaryPair>? = null

    @Value("\${s3.bucket}")
    private lateinit var s3BucketName: String

    @Value("\${s3.prefix.folder:business-data/mongo}")
    private lateinit var s3PrefixFolder: String

    @Value("\${s3.suffixes.csv}")
    lateinit var s3SuffixesCsv: String

    @Value("\${filename.format.regex:[\\w-]+\\.[\\w-]+\\.[0-9]+\\.json}")
    private lateinit var fileNameFormat: String

    @Value("\${filename.format.data.extension:\\.gz\\.enc$}")
    private lateinit var fileNameFormatDataExtension: String

    @Value("\${filename.format.metadata.extension:\\.encryption\\.json$}")
    private lateinit var fileNameFormatMetadataExtension: String

    @Autowired
    private lateinit var s3Helper: S3Helper

    override fun read(): InputStreamPair? {
        val iterator = getS3ObjectSummariesIterator(s3client, s3BucketName)
        return if (iterator.hasNext()) {
            iterator.next().let {
                logger.info("Found s3 summary pair", "s3_object_summary_pair", "$it")
                val dataInputStream = it.data?.let { it1 -> s3Helper.getS3ObjectInputStream(it1, s3client, s3BucketName) }
                val metadataInputStream = it.metadata?.let { it1 -> s3Helper.getS3ObjectInputStream(it1, s3client, s3BucketName) }
                return InputStreamPair(dataInputStream!!, metadataInputStream!!, it.data.key, it.data.size)
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
    private fun getS3ObjectSummariesIterator(awsS3Client: AmazonS3, bucketName: String): ListIterator<S3ObjectSummaryPair> {
        if (null == iterator) {
            if (s3SuffixesCsv.isBlank()) {
                throw UnsupportedOperationException("Parameter s3SuffixesCsv must be set but was '$s3SuffixesCsv'")
            }
            val allSuffixes = s3SuffixesCsv.trim().split(",")
            val allPairs = mutableListOf<S3ObjectSummaryPair>()
            allSuffixes.forEach { suffix ->
                val somePairs = getS3ObjectSummariesList(awsS3Client, bucketName, "$s3PrefixFolder/$suffix")
                allPairs.addAll(somePairs)
            }
            iterator = allPairs.toList().listIterator()
        }
        return iterator!!
    }

    @Synchronized
    private fun getS3ObjectSummariesList(awsS3Client: AmazonS3, bucketName: String, fullPrefix: String): List<S3ObjectSummaryPair> {

        logger.info("Looking for s3 objects", "s3_location", "s3://$bucketName/$fullPrefix")
        val request = ListObjectsV2Request().apply {
            withBucketName(bucketName)
            withPrefix(fullPrefix)
        }

        var results: ListObjectsV2Result?
        val objectSummaries: MutableList<S3ObjectSummary> = mutableListOf()

        do {
            logger.info("Getting paginated results", "s3_location", "s3://$bucketName/$fullPrefix")
            results = s3Helper.listObjectsV2Result(awsS3Client, request, objectSummaries)
            request.continuationToken = results?.nextContinuationToken
        }
        while (results != null && results.isTruncated)

        logger.info("Found objects in s3", "s3_objects_found", "s3://$bucketName/$fullPrefix", "s3_location", "${objectSummaries.size}")
        val objectSummaryKeyMap = objectSummaries.map { it.key to it }.toMap()
        val keyPairs =
            keyPairGenerator.generateKeyPairs(objectSummaries.map { it.key },
                fileNameFormat.toRegex(),
                fileNameFormatDataExtension.toRegex(),
                fileNameFormatMetadataExtension.toRegex())

        val pairs = keyPairs.map {
            val obj = objectSummaryKeyMap[it.dataKey]
            val meta = objectSummaryKeyMap[it.metadataKey]
            S3ObjectSummaryPair(obj, meta)
        }
        logger.info("Found valid key pairs", "s3_keypairs_found", "${pairs.size}", "s3_location", "s3://$bucketName/$fullPrefix")
        return pairs
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(S3Reader::class.toString())
    }
}
