package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import app.utils.logging.JsonLoggerWrapper
import app.utils.logging.correlation_id
import com.google.gson.JsonObject
import com.jcabi.manifests.Manifests
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.env.Environment
import org.springframework.core.env.get
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*

@Component
class MessageProducer {

    fun produceMessage(jsonObject: JsonObject,
                       id: String,
                       idWasModified: Boolean,
                       lastModifiedDateTime: String,
                       lastModifiedDateTimeWasModified: Boolean,
                       encryptionResult: EncryptionResult,
                       dataKeyResult: DataKeyResult,
                       database: String,
                       collection: String): String {
        val type = jsonObject.getAsJsonPrimitive("@type")?.asString ?: "MONGO_IMPORT"

        // SimpleDateFormat is not thread-safe so we need a new one every time
        val standardDateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
        val timestamp = standardDateFormat.format(Date())
        val messageId = if (idWasModified) """"$id"""" else id

        return """{
   "unitOfWorkId": "$unitOfWorkId",
   "timestamp": "$timestamp",
   "traceId": "$correlation_id",
   "@type": "HDI",
   "version": "$hdiVersion",
   "message": {
       "@type": "$type",
       "_id": $messageId,
       "mongo_format_stripped_from_id": $idWasModified,
       "last_modified_date_time_was_altered": $lastModifiedDateTimeWasModified,
       "_lastModifiedDateTime": "$lastModifiedDateTime",
       "collection" : "$collection",
       "db": "$database",
       "dbObject": "${encryptionResult.encrypted}",
       "encryption": {
           "keyEncryptionKeyId": "${dataKeyResult.dataKeyEncryptionKeyId}",
           "initialisationVector": "${encryptionResult.initialisationVector}",
           "encryptedEncryptionKey": "${dataKeyResult.ciphertextDataKey}"
       }
   }
}"""
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(MessageProducer::class.toString())
    }

    private val hdiVersion: String by lazy {
        val valueFromManifest = try {
            Manifests.read("Hdi-Version")
        }
        catch (e: Exception) {
            null
        }
        val valueFromArguments = environment.get("hdi.version")
        val result = valueFromManifest ?: valueFromArguments ?: "No hdi version available."
        logger.info("Getting HDI version","value_from_manifest", "$valueFromManifest", "value_from_arguments", "$valueFromArguments", "result_version", result)
        result
    }

    private val unitOfWorkId by lazy {
        UUID.randomUUID().toString()
    }

    @Autowired
    private lateinit var environment: Environment
}
