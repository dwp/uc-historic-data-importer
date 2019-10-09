package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

@Component
class DbObjectEncryptor {

    @Autowired
    private lateinit var cipherService: CipherService

    @Autowired
    private lateinit var keyService: KeyService

    fun encryptDbObject(dataKeyResult: DataKeyResult, line: String, fileName: String, id: String?): EncryptionResult {
        try {
            return cipherService.encrypt(dataKeyResult.plaintextDataKey,
                line.toByteArray())
        }
        catch (e: Exception) {
            logger.error("Error while encrypting db object id $id in file  ${fileName}: $e")
            throw e
        }
    }

    fun getDataKey(fileName: String): DataKeyResult {
        try {
            return keyService.batchDataKey()
        }
        catch (e: Exception) {
            logger.error("Error while creating data key for the file  $fileName: $e")
            throw e
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DbObjectEncryptor::class.toString())
    }
}

