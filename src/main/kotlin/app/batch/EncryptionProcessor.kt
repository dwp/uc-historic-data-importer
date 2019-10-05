package app.batch

import app.configuration.HttpClientProvider
import app.domain.InputStreamPair
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class EncryptionProcessor(private val httpClientProvider: HttpClientProvider) : ItemProcessor<InputStreamPair, InputStreamPair> {

    override fun process(item: InputStreamPair): InputStreamPair? {
        logger.info("httpClientProvider: '$httpClientProvider'.")
        return item
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EncryptionProcessor::class.toString())
    }
}
