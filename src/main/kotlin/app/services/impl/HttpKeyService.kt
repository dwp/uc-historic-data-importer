package app.services.impl

import app.configuration.HttpClientProvider
import app.domain.DataKeyResult
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.services.KeyService
import app.utils.UUIDGenerator
import app.utils.logging.JsonLoggerWrapper
import com.google.gson.Gson
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.retry.annotation.Backoff
import org.springframework.retry.annotation.Retryable
import org.springframework.stereotype.Service
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.Reader
import java.net.URLEncoder

@Service
class HttpKeyService(
    private val httpClientProvider: HttpClientProvider,
    private val uuidGenerator: UUIDGenerator) : KeyService {

    @Override
    @Retryable(value = [DataKeyServiceUnavailableException::class],
        maxAttempts = maxAttempts,
        backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Throws(DataKeyServiceUnavailableException::class, DataKeyDecryptionException::class)
    override fun decryptKey(encryptionKeyId: String, encryptedKey: String): String {
        val dksCorrelationId = uuidGenerator.randomUUID()
        logger.info("Calling decryptKey: encryptedKey:'$encryptedKey', keyEncryptionKeyId: '$encryptionKeyId', dks_correlation_id: '$dksCorrelationId'")
        try {
            val cacheKey = "$encryptedKey/$encryptionKeyId"
            return if (decryptedKeyCache.containsKey(cacheKey)) {
                decryptedKeyCache[cacheKey]!!
            }
            else {
                httpClientProvider.client().use { client ->
                    val dksUrl = "$dataKeyServiceUrl/datakey/actions/decrypt?keyId=${URLEncoder.encode(encryptionKeyId, "US-ASCII")}"
                    val dksUrlWithCorrelationId = "$dksUrl&correlationId=$dksCorrelationId"
                    logger.info("Calling decryptKey: dks_url: '$dksUrl', dks_correlation_id: '$dksCorrelationId'")
                    val httpPost = HttpPost(dksUrlWithCorrelationId)
                    httpPost.entity = StringEntity(encryptedKey, ContentType.TEXT_PLAIN)
                    client.execute(httpPost).use { response ->
                        val statusCode = response.statusLine.statusCode
                        logger.info("Calling decryptKey: dks_url: '$dksUrl', dks_correlation_id: '$dksCorrelationId' returned status_code: '$statusCode'.")
                        return when (statusCode) {
                            200 -> {
                                val entity = response.entity
                                val text = BufferedReader(InputStreamReader(response.entity.content) as Reader?).use(BufferedReader::readText)
                                EntityUtils.consume(entity)
                                val dataKeyResult = Gson().fromJson(text, DataKeyResult::class.java)
                                decryptedKeyCache[cacheKey] = dataKeyResult.plaintextDataKey
                                dataKeyResult.plaintextDataKey
                            }
                            400 ->
                                throw DataKeyDecryptionException(
                                    "Decrypting encryptedKey: '$encryptedKey' with keyEncryptionKeyId: '$encryptionKeyId', dks_correlation_id: '$dksCorrelationId' data key service returned status code '$statusCode'")
                            else ->
                                throw DataKeyServiceUnavailableException(
                                    "Decrypting encryptedKey: '$encryptedKey' with keyEncryptionKeyId: '$encryptionKeyId', dks_correlation_id: '$dksCorrelationId' data key service returned status code '$statusCode'")
                        }
                    }
                }
            }
        }
        catch (ex: Exception) {
            when (ex) {
                is DataKeyDecryptionException, is DataKeyServiceUnavailableException -> {
                    throw ex
                }
                else -> throw DataKeyServiceUnavailableException("Error contacting data key service: '$ex', dks_correlation_id: '$dksCorrelationId'")
            }
        }
    }

    @Override
    @Retryable(value = [DataKeyServiceUnavailableException::class],
        maxAttempts = maxAttempts,
        backoff = Backoff(delay = initialBackoffMillis, multiplier = backoffMultiplier))
    @Throws(DataKeyServiceUnavailableException::class)
    override fun batchDataKey(): DataKeyResult {
        val dksCorrelationId = uuidGenerator.randomUUID()
        val dksUrl = "$dataKeyServiceUrl/datakey"
        val dksUrlWithCorrelationId = "$dksUrl?correlationId=$dksCorrelationId"
        try {
            logger.info("Calling batchDataKey", "dks_url", dksUrl, "dks_correlation_id", dksCorrelationId)
            httpClientProvider.client().use { client ->
                client.execute(HttpGet(dksUrlWithCorrelationId)).use { response ->
                    val statusCode = response.statusLine.statusCode
                    logger.info("Called batchDataKey", "dks_url", dksUrl, "dks_correlation_id", dksCorrelationId, "status_code", "$statusCode")
                    return if (statusCode == 201) {
                        val entity = response.entity
                        val result = BufferedReader(InputStreamReader(entity.content))
                            .use(BufferedReader::readText).let {
                                Gson().fromJson(it, DataKeyResult::class.java)
                            }
                        EntityUtils.consume(entity)
                        result
                    }
                    else {
                        throw DataKeyServiceUnavailableException("Calling batchDataKey: dks_url: '$dksUrl', dks_correlation_id: '$dksCorrelationId' returned status_code '$statusCode'")
                    }
                }
            }
        }
        catch (ex: Exception) {
            when (ex) {
                is DataKeyServiceUnavailableException -> {
                    throw ex
                }
                else -> throw DataKeyServiceUnavailableException("Error contacting data key service: '$ex', dks_correlation_id: '$dksCorrelationId'")
            }
        }
    }

    override fun clearCache() {
        this.decryptedKeyCache = mutableMapOf()
    }

    private var decryptedKeyCache = mutableMapOf<String, String>()

    @Value("\${data.key.service.url}")
    private lateinit var dataKeyServiceUrl: String

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(HttpKeyService::class.toString())
        // Will retry at 1s, 2s, 4s, 8s, 16s then give up (after a total of 31 secs)
        const val maxAttempts = 5
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }

}
