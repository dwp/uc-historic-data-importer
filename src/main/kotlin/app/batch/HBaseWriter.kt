package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import org.apache.hadoop.hbase.client.Connection
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.io.BufferedReader
import java.io.InputStreamReader

@Component
class HBaseWriter(private val connection: Connection) : ItemWriter<DecompressedStream> {

    @Autowired
    private lateinit var cipherService: CipherService

    @Autowired
    private lateinit var keyService: KeyService

    override fun write(items: MutableList<out DecompressedStream>) {
        items.forEach {
            val fileName = it.fileName
            val dataKeyResult: DataKeyResult = getDataKey(fileName)
            val reader = BufferedReader(InputStreamReader(it.inputStream))
            var line: String? = null
            var id: String?
            while ({ line = reader.readLine(); line }() != null) {
                try {
                    val parser: Parser = Parser.default()
                    val json = line?.let { notNullLine -> parser.parse(notNullLine) } as JsonObject
                    id = getId(json, fileName)?.toJsonString()
                    logger.info("Parsing DB object of id $id  in the file $fileName")
                }
                catch (e: Exception) {
                    logger.error("Error while parsing the file $fileName: $e")
                    continue
                }
                encryptDbObject(dataKeyResult, line!!, fileName, id)
            }
        }
    }

    fun getId(json: JsonObject, fileName: String): JsonObject? {
        return try {
            json.obj("_id")
        }
        catch (e: Exception) {
            logger.warn("DB object  does not contain _id field in the file $fileName")
            null
        }
    }

    fun encryptDbObject(dataKeyResult: DataKeyResult, line: String, fileName: String, id: String?): EncryptionResult {
        try {
            return cipherService.encrypt(dataKeyResult.plaintextDataKey,
                line.toByteArray())
        }
        catch (e: Exception) {
            DbObjectEncryptor.logger.error("Error while encrypting db object id $id in file  ${fileName}: $e")
            throw e
        }
    }

    fun getDataKey(fileName: String): DataKeyResult {
        try {
            return keyService.batchDataKey()
        }
        catch (e: Exception) {
            DbObjectEncryptor.logger.error("Error while creating data key for the file  $fileName: $e")
            throw e
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }

}
