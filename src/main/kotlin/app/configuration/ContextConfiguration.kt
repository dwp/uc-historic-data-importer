package app.configuration

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.http.impl.client.HttpClients
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.security.SecureRandom

@Configuration
class ContextConfiguration {

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

//    @Bean
//    fun localConnection(): Connection {
//
//        val connection = ConnectionFactory.createConnection(HBaseConfiguration.create().apply {
//            this.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum)
//            this.setInt("hbase.zookeeper.port", 2181)
//        })
//
//        addShutdownHook(connection)
//        return connection
//    }

    private fun addShutdownHook(connection: Connection) {
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("Closing hbase connection: '$connection'.")
                connection.close()
            }
        })
    }

    @Value("\${hbase.zookeeper.quorum}")
    private lateinit var hbaseZookeeperQuorum: String

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ContextConfiguration::class.toString())
    }
}
