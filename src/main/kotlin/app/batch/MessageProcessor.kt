package app.batch

import app.services.CipherService
import app.services.KeyService
import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import com.beust.klaxon.Parser
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader

@Component
class MessageProcessor : ItemProcessor<InputStream, InputStream> {

    @Autowired
    private lateinit var keyService: KeyService
    @Autowired
    private lateinit var cipherService: CipherService
    @Value("\${messages.count.per.datakey:1000}")
    val messagesCountPerDataKey: Long? = null

    override fun process(item: InputStream): InputStream? {
        var count: Long = 0
        val reader = BufferedReader(InputStreamReader(item))
        var line: String? = null
        while ({ var line = reader.readLine(); line }() != null) {
            try {
                try {
                    val parser: Parser = Parser.default()
                    line?.let { parser.parse(it) } as JsonObject
                }
                catch (e: KlaxonException) {
                    logger.warn("Error while parsing json: $e")
                    continue
                }
                count++
                encryptDbObject(count, line)
            }
            catch (e: Exception) {
                logger.warn("Error while parsing json: $e")

            }
        }
        return null
    }

    private fun encryptDbObject(count: Long, line: String) {

        var dataKeyResult: DataKeyResult = getDataKey()
        if (messagesCountPerDataKey?.let { count.rem(it) } == 0L) {
            dataKeyResult = getDataKey()
        }
        logger.info("dataKeyResult: $dataKeyResult")
        this.cipherService.encrypt(dataKeyResult.plaintextDataKey,
            line.toByteArray())
    }


    private fun getDataKey(): DataKeyResult {
        return keyService.batchDataKey()
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MessageProcessor::class.toString())
    }
}

