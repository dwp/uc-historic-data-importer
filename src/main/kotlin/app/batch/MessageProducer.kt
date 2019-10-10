package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import com.beust.klaxon.JsonObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProducer {
    fun produceMessage(jsonObject: JsonObject,
                       encryptionResult: EncryptionResult,
                       dataKeyResult: DataKeyResult,
                       database: String,
                       collection: String): String {

        val id = jsonObject.obj("_id")?.toJsonString()!!
        val modified = jsonObject.obj("_lastModifiedDateTime")//?.obj("\$date")
        val date = modified?.get("\$date")
        logger.info("$date")
        return if (modified != null) {
                """{
                |   "message": {
                |       "@type": "MONGO_UPDATE",
                |       "_id": $id,
                |       "_lastModifiedDateTime": "$date",
                |       "collection" : "$collection",
                |       "db": "$database",
                |       "dbObject": "${encryptionResult.encrypted}",
                |       "encryption": {
                |           "keyEncryptionKeyId": "${dataKeyResult.dataKeyEncryptionKeyId}",
                |           "initialisationVector": "${encryptionResult.initialisationVector}",
                |           "encryptedEncryptionKey": "${dataKeyResult.ciphertextDataKey}"
                |       }
                |   }
                |}""".trimMargin()
        }
        else {
            logger.error("No '_lastModifiedDateTime' in record '$id' from '$database/$collection'.")
            ""
        }
    }


    companion object {
        val logger: Logger = LoggerFactory.getLogger(MessageProducer::class.toString())
    }

}