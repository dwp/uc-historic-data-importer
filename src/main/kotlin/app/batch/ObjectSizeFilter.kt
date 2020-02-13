package app.batch

import app.domain.InputStreamPair
import app.utils.logging.JsonLoggerWrapper
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class ObjectSizeFilter(private val maxSize: Long) : ItemProcessor<InputStreamPair, InputStreamPair> {


    override fun process(item: InputStreamPair): InputStreamPair? {

        return if (item.objectSize <= maxSize) {
            item
        }
        else {
            logger.error("Rejecting oversized file", "s3_location", item.s3key, "s3_object_size", "${item.objectSize}", "max_size", "$maxSize")
            null
        }
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ObjectSizeFilter::class.toString())
    }
}
