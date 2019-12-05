package app.batch

import app.domain.DataKeyResult
import app.domain.EncryptionResult
import com.google.gson.Gson
import com.google.gson.JsonObject
import com.jcabi.manifests.Manifests
import org.apache.commons.lang3.StringUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.env.Environment
import org.springframework.core.env.get
import org.springframework.stereotype.Component
import java.text.SimpleDateFormat
import java.util.*

@Component
class MessageProducer {
    fun produceMessage(jsonObject: JsonObject,
                       id: String,
                       encryptionResult: EncryptionResult,
                       dataKeyResult: DataKeyResult,
                       database: String,
                       collection: String): String {
        var lastModified = jsonObject.getAsJsonObject("_lastModifiedDateTime")
                ?.getAsJsonPrimitive("\$date")
                ?.asString
                ?: "1980-01-01T00:00:00.000Z"

        lastModified = if (StringUtils.isNotBlank(lastModified)) lastModified else "1980-01-01T00:00:00.000Z"

        //println("lastModified: '$lastModified'.")
        val type = jsonObject.getAsJsonPrimitive("@type")?.asString ?: "MONGO_UPDATE"
        val timestamp = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").format(Date())


        return """{
   "unitOfWorkId": "$unitOfWorkId",
   "timestamp": "$timestamp",
   "traceId": "$traceId",
   "@type": "HDI",
   "version": "$hdiVersion",
   "message": {
       "@type": "$type",
       "_id": $id,
       "_lastModifiedDateTime": "$lastModified",
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
        val logger: Logger = LoggerFactory.getLogger(MessageProducer::class.toString())
    }

    @Value("\${trace.id}")
    private lateinit var traceId: String

    private val hdiVersion: String by lazy {
        val valueFromManifest = try { Manifests.read("Hdi-Version") } catch (e: Exception) { null }
        val valueFromArguments = environment.get("hdi.version")
        logger.info("valueFromManifest: '$valueFromManifest', valueFromArguments: '$valueFromArguments'.")
        valueFromManifest ?: valueFromArguments ?: "No hdi version available."
    }

    private val unitOfWorkId by lazy {
        UUID.randomUUID().toString()
    }

    @Autowired
    private lateinit var environment: Environment
}
