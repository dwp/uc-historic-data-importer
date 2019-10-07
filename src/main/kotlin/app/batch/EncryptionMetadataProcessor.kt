package app.batch

import app.domain.EncryptedStream
import app.domain.EncryptionMetadata
import app.domain.InputStreamPair
import app.exceptions.MetadataException
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

/**
 * Intermediate processor - reads, parses the input stream of
 * encryption metadata and passes it down the chain.
 */
@Component
class EncryptionMetadataProcessor : ItemProcessor<InputStreamPair, EncryptedStream> {

    override fun process(item: InputStreamPair): EncryptedStream {
        try {
            return EncryptedStream(item.dataInputStream, encryptionMetadata(item), item.s3key)
        }
        catch (e: Exception) {
            val message = "Failed to parse encryption metadata for '${item.s3key}': '${e.message}'."
            logger.error(message)
            throw MetadataException(message, e)
        }
    }

    private fun encryptionMetadata(item: InputStreamPair) =
            ObjectMapper().readValue(item.metadataInputStream,
                                        EncryptionMetadata::class.java)


    companion object {
        val logger: Logger = LoggerFactory.getLogger(EncryptionMetadataProcessor::class.toString())
    }
}
