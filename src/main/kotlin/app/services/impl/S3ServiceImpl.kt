package app.services.impl

import app.services.S3Service
import app.utils.logging.JsonLoggerWrapper
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service

@Service
class S3ServiceImpl(private val amazonS3: AmazonS3): S3Service {

    @Retryable(value = [Exception::class],
            maxAttempts = maxAttempts,
            backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    override fun objectInputStream(bucket: String, key: String): S3ObjectInputStream = amazonS3.getObject(bucket, key).objectContent


    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(S3Service::class.toString())
        const val maxAttempts = 10
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }
}
