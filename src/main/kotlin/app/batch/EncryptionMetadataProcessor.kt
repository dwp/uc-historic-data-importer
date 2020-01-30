package app.batch

import app.domain.EncryptedStream
import app.domain.EncryptionMetadata
import app.domain.InputStreamPair
import app.exceptions.MetadataException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.commons.lang3.StringUtils
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
        } catch (e: MetadataException) {
            throw e
        } catch (e: Exception) {
            val message = "Failed to parse encryption metadata for '${item.s3key}'."
            logger.error(message)
            throw MetadataException(message, e)
        }
    }

    private fun encryptionMetadata(item: InputStreamPair): EncryptionMetadata {
        val metadata = ObjectMapper().readValue(item.metadataInputStream,
            EncryptionMetadata::class.java)
        assertNotBlank(item.s3key, "encryptedEncryptionKey", metadata.encryptedEncryptionKey)
        assertNotBlank(item.s3key, "initialisationVector", metadata.initialisationVector)
        assertNotBlank(item.s3key, "keyEncryptionKeyId", metadata.keyEncryptionKeyId)
        return metadata
    }

    private fun assertNotBlank(s3key: String, field: String, value: String) {
        if (StringUtils.isBlank(value)) {
            throw MetadataException("Failed to process encryption metadata for '$s3key': '$field' encryption metadata field must not be blank.")
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EncryptionMetadataProcessor::class.toString())
    }
}
