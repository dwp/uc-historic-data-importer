package app.batch

import app.configuration.HttpClientProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class EncryptionProcessor(private val httpClientProvider: HttpClientProvider): ItemProcessor<EncryptedStream, EncryptedStream> {

    override fun process(item: EncryptedStream): EncryptedStream? {
        logger.info("httpClientProvider: '$httpClientProvider'.")
        return item
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EncryptionProcessor::class.toString())
    }
}
