package app.batch

import com.amazonaws.services.s3.AmazonS3
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemReader
import org.springframework.stereotype.Component

@Component
class S3Reader(private val s3client: AmazonS3): ItemReader<String> {

    override fun read(): String? {
        logger.info("s3client: '$s3client'.")
        return if (count++ < 10) {
            "item-$count"
        }
        else {
            null
        }
    }
    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }

    private var count = 0
}