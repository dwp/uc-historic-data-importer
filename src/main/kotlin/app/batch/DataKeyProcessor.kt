package app.batch

import app.domain.EncryptedStream
import app.exceptions.DataKeyDecryptionException
import app.services.KeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
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

            if (item.encryptionMetadata.initialisationVector == "AAAAAAAAAAAAAAAAAAAAAA==") {
                encryptionMetadata.plaintextDatakey = "AAAAAAAAAAAAAAAAAAAAAA=="
            }
            else {
                val plaintextKey = keyService.decryptKey(encryptionMetadata.keyEncryptionKeyId,
                        encryptionMetadata.encryptedEncryptionKey)
                encryptionMetadata.plaintextDatakey = plaintextKey
            }

            return item
        } catch (e: DataKeyDecryptionException) {
            val message = "Failed to decrypt '${item.s3key}': '${e.message}'."
            logger.error(message)
            throw e
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DataKeyProcessor::class.toString())
    }

}
