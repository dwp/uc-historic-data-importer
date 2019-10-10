package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import com.beust.klaxon.JsonObject
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class MessageProducer {
    fun produceMessage(jsonObject: JsonObject,
                       encryptionResult: EncryptionResult,
                       dataKeyResult: DataKeyResult,
                       database: String,
                       collection: String): String {
        val id = jsonObject.obj("_id")?.toJsonString()!!
        val modified = jsonObject.obj("_lastModifiedDateTime")
        val date = modified?.get("\$date")
        val dateStr = if (date != null) date as String else ""
        val type = jsonObject.get("@type") ?: "MONGO_UPDATE"
        return if (StringUtils.isNotBlank(dateStr)) {
                """{
                |   "message": {
                |       "@type": "$type",
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