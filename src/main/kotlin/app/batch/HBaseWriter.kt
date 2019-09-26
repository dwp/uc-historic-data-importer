package app.batch

import org.apache.hadoop.hbase.client.Connection
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.stereotype.Component

@Component
class HBaseWriter(private val connection: Connection): ItemWriter<String> {
    override fun write(items: MutableList<out String>) {
        logger.info("connection: '$connection'.")
        items.forEach {
            logger.info("it: '$it'.")
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }

}