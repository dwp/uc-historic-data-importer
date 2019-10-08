package app.services.impl

import app.batch.DataKeyResult
import app.configuration.HttpClientProvider
import app.exceptions.DataKeyServiceUnavailableException
import app.services.KeyService
import com.google.gson.Gson
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Service
import java.io.BufferedReader
import java.io.InputStreamReader

@Service
@Profile("httpDataKeyService")
class HttpKeyService(private val httpClientProvider: HttpClientProvider) : KeyService {

    override fun batchDataKey(): DataKeyResult {
        httpClientProvider.client().use { client ->
            client.execute(HttpGet("$dataKeyServiceUrl/datakey")).use { response ->
                return if (response.statusLine.statusCode == 201) {
                    val entity = response.entity
                    val result = BufferedReader(InputStreamReader(entity.content))
                        .use(BufferedReader::readText).let {
                            Gson().fromJson(it, DataKeyResult::class.java)
                        }
                    EntityUtils.consume(entity)
                    result
                } else {
                    throw DataKeyServiceUnavailableException(
                        "DataKeyService returned status code '${response.statusLine.statusCode}'.")
                }
            }
        }
    }

    fun clearCache() {
        this.decryptedKeyCache = mutableMapOf()
    }

    private var decryptedKeyCache = mutableMapOf<String, String>()

    @Value("\${data.key.service.url}")
    private lateinit var dataKeyServiceUrl: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HttpKeyService::class.toString())
    }

}
