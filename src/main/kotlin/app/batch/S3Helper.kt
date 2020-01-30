package app.batch

import app.exceptions.S3Exception
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.model.S3ObjectSummary
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Component

@Component
class S3Helper {

    @Retryable(value = [S3Exception::class],
        maxAttempts = maxAttempts,
        backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Throws(S3Exception::class)
    fun getS3ObjectInputStream(os: S3ObjectSummary, s3Client: AmazonS3, bucketName: String): S3ObjectInputStream {
        try {
            return s3Client.getObject(bucketName, os.key).objectContent
        } catch (ex: Exception) {
            throw S3Exception("Error retrieving object from S3")
        }
    }

    @Retryable(value = [S3Exception::class],
        maxAttempts = maxAttempts,
        backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Throws(S3Exception::class)
    fun listObjectsV2Result(awsS3Client: AmazonS3, request: ListObjectsV2Request, objectSummaries: MutableList<S3ObjectSummary>): ListObjectsV2Result? {
        try {
            var results1 = awsS3Client.listObjectsV2(request)
            objectSummaries.addAll(results1.objectSummaries)
            return results1
        } catch (ex: Exception) {
            throw S3Exception("Error retrieving object summary from S3")
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(S3Helper::class.toString())
        const val maxAttempts = 5
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }
}

