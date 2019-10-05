package app.batch

import app.domain.EncryptedStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.stereotype.Component
import java.io.BufferedInputStream
import java.io.InputStream

@Component
class HBaseWriter(/*private val connection: Connection*/) : ItemWriter<InputStream> {
    override fun write(items: MutableList<out InputStream>) {
//        logger.info("connection: '$connection'.")
        items.forEach {
            it.buffered().use {
                val contents = it.readBytes()
                println("${String(contents)}")
            }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }

}
