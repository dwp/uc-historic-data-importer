package app.configuration

import app.batch.HbaseClient
import app.batch.migrate
import org.apache.http.impl.client.HttpClients
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.security.SecureRandom
import javax.crypto.Cipher

@Configuration
class ContextConfiguration {

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
    fun hbaseClient(): HbaseClient {
        val hbase = HbaseClient.connect()
        hbase.migrate()
        return hbase
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ContextConfiguration::class.toString())
    }
}
