package app.batch

import app.domain.EncryptedStream
import app.exceptions.DataKeyDecryptionException
import app.services.KeyService
import app.utils.logging.JsonLoggerWrapper
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

/**
 * Adds the decrypted key to the encryption metadata enabling decryption of the main data file.
 */
@Component
class DataKeyProcessor(val keyService: KeyService) : ItemProcessor<EncryptedStream, EncryptedStream> {

    override fun process(item: EncryptedStream): EncryptedStream {
        try {
            val encryptionMetadata = item.encryptionMetadata
            val plaintextKey = keyService.decryptKey(encryptionMetadata.keyEncryptionKeyId,
                encryptionMetadata.encryptedEncryptionKey)
            encryptionMetadata.plaintextDatakey = plaintextKey
            return item
        }
        catch (e: DataKeyDecryptionException) {
            logger.error("Failed to decrypt key", "s3_location", item.s3key, "error_message", "${e.message}")
            throw e
        }
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(DataKeyProcessor::class.toString())
    }

}
