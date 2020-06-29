package app.configuration

import app.batch.HbaseClient
import app.utils.logging.JsonLoggerWrapper
import org.apache.http.impl.client.HttpClients
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.security.SecureRandom
import javax.crypto.Cipher

@Configuration
class ContextConfiguration {

    @Bean
    fun maxSize() = maxSize.toLong()


    @Bean
    fun cipherInstanceProvider(): CipherInstanceProvider {
        return object : CipherInstanceProvider {
            override fun cipherInstance(): Cipher {
                return Cipher.getInstance("AES/CTR/NoPadding", "BC")
            }
        }
    }

    @Bean
    @Profile("insecureHttpClient")
    fun insecureHttpClientProvider() = object : HttpClientProvider {
        override fun client() = HttpClients.createDefault()!!
    }

    @Bean
    @Profile("strongRng")
    fun secureRandom() = SecureRandom.getInstanceStrong()!!

    @Bean
    @Profile("weakRng")
    fun weakRandom() = SecureRandom.getInstance("SHA1PRNG")!!

    @Bean
    @Profile("hbase")
    fun hbaseClient() = HbaseClient.connect()

    @Value("\${object.maximum.size:2200000000}")
    lateinit var maxSize: String

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(ContextConfiguration::class.toString())
    }
}
