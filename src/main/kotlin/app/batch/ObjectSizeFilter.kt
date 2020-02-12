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
            logger.error("""Rejecting '${item.s3key}': size - ${item.objectSize} greater than the maximum allowed: $maxSize.""")
            null
        }
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ObjectSizeFilter::class.toString())
    }
}
