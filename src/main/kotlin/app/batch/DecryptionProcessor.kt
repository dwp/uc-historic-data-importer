package app.batch

import app.configuration.HttpClientProvider
import app.domain.EncryptedStream
import app.exceptions.DecryptionException
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import java.io.InputStream
import java.security.Key
import java.security.Security
import java.util.*
import javax.crypto.Cipher
import javax.crypto.CipherInputStream
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

@Component
class DecryptionProcessor: ItemProcessor<EncryptedStream, InputStream> {

    init {
        Security.addProvider(BouncyCastleProvider())
    }

    override fun process(item: EncryptedStream): InputStream {
        try {
            val dataKey = item.encryptionMetadata.plaintextDatakey
            val iv = item.encryptionMetadata.iv
            val inputStream = item.dataInputStream
            val keySpec: Key = SecretKeySpec(dataKey.toByteArray(), "AES")
            val cipher = Cipher.getInstance(cipherAlgorithm, "BC").apply {
                init(Cipher.DECRYPT_MODE, keySpec, IvParameterSpec(Base64.getDecoder().decode(iv)))
            }
            return CipherInputStream(Base64.getDecoder().wrap(inputStream), cipher)
        }
        catch (e: Exception) {
            val message = "Failed to decrypt data in '${item.s3key}': ${e.message}."
            logger.error(message)
            throw DecryptionException(message, e)
        }
    }

    private val cipherAlgorithm = "AES/CTR/NoPadding"

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DecryptionProcessor::class.toString())
    }
}
