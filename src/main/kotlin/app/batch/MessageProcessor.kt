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

    override fun process(item: InputStream): InputStream? {
        val dataKeyResult = keyService.batchDataKey()
        logger.info("dataKeyResult: '$dataKeyResult'.")
        val reader = BufferedReader(InputStreamReader(item))
        while (reader.ready()) {
            val line = reader.readLine()
            try {
                val parser: Parser = Parser.default()
                parser.parse(line) as JsonObject
            }
            catch (e: KlaxonException) {
                logger.warn("Error while parsing json: '${e}")
            }
            val encryptionResult =
                this.cipherService.encrypt(dataKeyResult.plaintextDataKey,
                    line.toByteArray())
        }
        return null
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(MessageProcessor::class.toString())
    }
}

