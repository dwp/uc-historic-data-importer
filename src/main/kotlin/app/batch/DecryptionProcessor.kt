package app.batch

import app.configuration.HttpClientProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class DecryptionProcessor(private val httpClientProvider: HttpClientProvider): ItemProcessor<String, String> {

    override fun process(item: String): String? {
        logger.info("httpClientProvider: '$httpClientProvider'.")
        return "{ \"item\": \"$item\" }"
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DecryptionProcessor::class.toString())
    }
}
