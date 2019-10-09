package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
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
    private lateinit var dbObjectGenerator: DbObjectEncryptor

    override fun write(items: MutableList<out DecompressedStream>) {
        items.forEach {
            val fileName = it.fileName
            val dataKeyResult: DataKeyResult = dbObjectGenerator.getDataKey(fileName)
            val reader = BufferedReader(InputStreamReader(it.inputStream))
            var line: String? = null
            var id: String? = null
            while ({ line = reader.readLine(); line }() != null) {
                try {
                    val parser: Parser = Parser.default()
                    val json = line?.let { notNullLine -> parser.parse(notNullLine) } as JsonObject
                    id = getId(json)?.toJsonString()
                    
                }
                catch (e: Exception) {
                    logger.error("Error while parsing id $id  in the file $fileName: $e ")
                    continue
                }
                dbObjectGenerator.encryptDbObject(dataKeyResult, line!!, fileName, id)
            }
        }
    }

    fun getId(json: JsonObject): JsonObject? {
        return try {
             json.obj("_id")
        } catch (e: Exception) {
            logger.warn("DB object  does not contain _id field")
            null
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }

}
