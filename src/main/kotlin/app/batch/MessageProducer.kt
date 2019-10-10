package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProducer {
    fun produceMessage(encryptionResult: EncryptionResult, dataKeyResult: DataKeyResult, filename: String): String {
        logger.info("encryptionResult: '$encryptionResult'.")
        logger.info("dataKeyResult: '$dataKeyResult'.")
        logger.info("filename: '$filename'.")
        val filenamePattern = """(?<database>[a-z-]+)\.(?<collection>[a-z-]+)\.\d+\.json\.gz\.enc$"""
        val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE);
        val matchResult = filenameRegex.find(filename)
        return if  (matchResult != null) {
            val groups = matchResult.groups
            val database = groups[1]!!.value
            val collection = groups[2]!!.value
            logger.info("database: '$database'")
            logger.info("collection: '$collection'.")
            """{
            |   "message": {
            |       "@type": "TODO",
            |       "_id": "TODO",
            |       "_lastModifiedDateTime": "TODO",
            |       "collection" : "$collection",
            |       "db": "$database",
            |       "dbObject": "${encryptionResult.encrypted}",
            |       "encryption": {
            |           "keyEncryptionKeyId": "${dataKeyResult.dataKeyEncryptionKeyId}",
            |           "initialisationVector": "${encryptionResult.initialisationVector}",
            |           "encryptedEncryptionKey": "${dataKeyResult.ciphertextDataKey}"
            |       }
            |   }
            }""".trimMargin()
        }
        else {
            ""
        }

    }


    companion object {
        val logger: Logger = LoggerFactory.getLogger(MessageProducer::class.toString())
    }

}