package app.batch

import app.configuration.CipherInstanceProvider
import app.domain.DecryptedStream
import app.domain.EncryptedStream
import app.exceptions.DecryptionException
import app.utils.logging.JsonLoggerWrapper
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import java.security.Key
import java.security.Security
import java.util.*
import javax.crypto.Cipher
import javax.crypto.CipherInputStream
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

@Component
class DecryptionProcessor(val cipherInstanceProvider: CipherInstanceProvider) : ItemProcessor<EncryptedStream, DecryptedStream> {

    init {
        Security.addProvider(BouncyCastleProvider())
    }

    override fun process(item: EncryptedStream): DecryptedStream {
        try {
            val dataKey = item.encryptionMetadata.plaintextDatakey
            val iv = item.encryptionMetadata.initialisationVector
            val inputStream = item.dataInputStream
            val keySpec: Key = SecretKeySpec(Base64.getDecoder().decode(dataKey), "AES")

            val cipher = cipherInstanceProvider.cipherInstance().apply {
                init(Cipher.DECRYPT_MODE, keySpec, IvParameterSpec(Base64.getDecoder().decode(iv)))
            }

            return DecryptedStream(CipherInputStream(inputStream, cipher), item.s3key, keySpec, iv)
        }
        catch (e: Exception) {
            val message = "Failed to decrypt data in '${item.s3key}': ${e.message}."
            logger.error(message)
            throw DecryptionException(message, e)
        }
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(DecryptionProcessor::class.toString())
    }
}
