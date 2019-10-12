package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.KeyService
import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hbase.client.Connection
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import java.io.BufferedOutputStream
import java.io.BufferedReader
import java.io.FileOutputStream
import java.io.InputStreamReader

@Component
class HBaseWriter(private val connection: Connection) : ItemWriter<DecompressedStream> {

    @Autowired
    private lateinit var cipherService: CipherService

    @Autowired
    private lateinit var keyService: KeyService

    override fun write(items: MutableList<out DecompressedStream>) {
        items.forEach {
            logger.info("Processing '${it.fileName}'.")
            val fileName = it.fileName
            val filenamePattern = """(?<database>[a-z0-9-]+)\.(?<collection>[a-z0-9-]+)\.\d+\.json\.gz\.enc$"""
            val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE)
            val matchResult = filenameRegex.find(fileName)
            if (matchResult != null) {
                val groups = matchResult.groups
                val database = groups[1]!!.value // can assert nun-null as it matched on the regex
                val collection = groups[2]!!.value // ditto
                val dataKeyResult: DataKeyResult = getDataKey(fileName)

                val buffer = ByteArray(1024)

                val outputStream = BufferedOutputStream(FileOutputStream("dump/${groups[0]!!.value}.gz"))

//                it.inputStream.copyTo(outputStream)
//                it.inputStream.close()
//                outputStream.close()
//                BufferedInputStream(it.inputStream).use {
//                    while (it.read(buffer)!= -1) {
//                        println(Charset.defaultCharset())
//                        println(String(buffer))
//                    }
//
//                }
                BufferedReader(InputStreamReader(it.inputStream)).use { reader ->
                    var line: String? = null
                    var lineNo = 0;
                    while ({ line = reader.readLine(); line }() != null) {
                        lineNo++
                        try {
                            val parser: Parser = Parser.default()
                            val stringBuilder = StringBuilder(line)
                            val json = parser.parse(stringBuilder) as JsonObject
                            val id = json.obj("_id")?.toJsonString()
                            if (StringUtils.isNotBlank(id)) {
                                val encryptionResult = encryptDbObject(dataKeyResult, line!!, fileName, id)
                                logger.info("Success '$fileName' line ${lineNo}.")
                                val message = MessageProducer().produceMessage(json, encryptionResult, dataKeyResult,
                                        database, collection)
                            //logger.info("Message: '$message'.")
                            }
                        }
                        catch (e: Exception) {
                            logger.error("Error while parsing record from '$fileName'.")
                        }
                    }
                }
            }
            logger.info("Processed '${it.fileName}'.")

        }
    }

    fun encryptDbObject(dataKeyResult: DataKeyResult, line: String, fileName: String, id: String?): EncryptionResult {
        try {
            return cipherService.encrypt(dataKeyResult.plaintextDataKey, line.toByteArray())
        }
        catch (e: Exception) {
            logger.error("Error while encrypting db object id $id in file  ${fileName}: $e")
            throw e
        }
    }

    fun getDataKey(fileName: String): DataKeyResult {
        try {
            return keyService.batchDataKey()
        }
        catch (e: Exception) {
            logger.error("Error while creating data key for the file  $fileName: $e")
            throw e
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HBaseWriter::class.toString())
    }
}
