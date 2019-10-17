
import app.batch.HbaseClient
import com.google.gson.Gson
import com.google.gson.JsonObject
import io.kotlintest.fail
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.Logger
import org.bouncycastle.jce.provider.BouncyCastleProvider
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.security.Key
import java.security.Security
import java.util.*
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec


class Kafka2HBaseSpec : StringSpec() {

    private val log = Logger.getLogger(Kafka2HBaseSpec::class.toString())

    init {
        Security.addProvider(BouncyCastleProvider())

        "Messages in Hbase should match the count 4" {
            val hbase = HbaseClient.connect()
            val scan = Scan()
            val count = hbase.connection.getTable(TableName.valueOf("k2hb:ingest")).use { table ->
                val scanner = table.getScanner(scan)
                val size = scanner.map { it }.size
                scanner.close()
                size
            }

            log.info("Messages count : $count")
            count shouldBe 4
        }

        "Topics in Hbase should match the count 3" {
            val hbase = HbaseClient.connect()
            val scan = Scan()
            val count = hbase.connection.getTable(TableName.valueOf("k2hb:ingest-topic")).use { table ->
                val scanner = table.getScanner(scan)
                val size = scanner.map { it }.size
                scanner.close()
                size
            }

            log.info("Topic count : $count")
            count shouldBe 3
        }

        "Messages in Hbase are decipherable" {
            val hbase = HbaseClient.connect()
            hbase.connection.getTable(TableName.valueOf("k2hb:ingest")).use { table ->
                val scan = Scan()
                val scanner = table.getScanner(scan)
                var result = scanner.next()
                do {
                    if (result == null) {
                        fail("No results returned - should be 4 records in the db")
                    }

                    val familyMap = result.noVersionMap

                    if (familyMap.size == 0){
                        fail("No column family on the table.")
                    }

                    familyMap.keys.forEach { columnFamilyName ->
                        val columnMap = familyMap.get(columnFamilyName)

                        if (columnMap == null) {
                            fail("No columns returned.")
                        }

                        if (columnMap.keys.size == 0) {
                            fail("No cells returned.")
                        }

                        columnMap?.keys?.forEach { columnName ->
                            val cell = columnMap[columnName] ?: ByteArray(0)
                            val json = cell.toString(Charset.defaultCharset())
                            val dataBlock = Gson().fromJson(json, JsonObject::class.java)
                            val messageInfo = dataBlock.getAsJsonObject("message")
                            val encryptedDbObject = messageInfo.getAsJsonPrimitive("dbObject")?.asString
                            val encryptionInfo = messageInfo.getAsJsonObject("encryption")
                            val encryptedEncryptionKey = encryptionInfo.getAsJsonPrimitive("encryptedEncryptionKey").asString
                            val initializationVector = encryptionInfo.getAsJsonPrimitive("initialisationVector").asString
                            if (encryptedDbObject != null) {
                                try {
                                    val decryptedKey = decryptedDatakey(encryptedEncryptionKey)
                                    val decryptedDbObject = decrypt(decryptedKey, initializationVector, encryptedDbObject)
                                    val jsonObject = Gson().fromJson(decryptedDbObject, JsonObject::class.java)
                                } catch (e: Exception) {
                                    fail("Decrypted db object should be parseable as json.")
                                }
                            }
                            else{
                                fail("No encrypted db object.")
                            }
                        }
                    }
                    result = scanner.next()
                }
                while (result != null)
            }
        }


    }

    fun decrypt(key: String, initializationVector: String, encrypted: String): String {
        val keySpec: Key = SecretKeySpec(key.toByteArray(), "AES")
        val cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC").apply {
            init(Cipher.DECRYPT_MODE, keySpec, IvParameterSpec(Base64.getDecoder().decode(initializationVector)))
        }
        val decodedBytes = Base64.getDecoder().decode(encrypted.toByteArray())
        val original = cipher.doFinal(decodedBytes)
        return String(original)
    }

    fun decryptedDatakey(ciphertextDataKey: String): String {
        val encoder = Base64.getEncoder()
        val decoder = Base64.getDecoder()
        val ciphertextDataKeyBuffer = ByteBuffer.wrap(decoder.decode(ciphertextDataKey))
        val decrypted = ciphertextDataKeyBuffer.array()
        reverse(decrypted)
        return encoder.encodeToString(decrypted)
    }

    fun reverse(array: ByteArray?) {
        if (array == null) {
            return
        }
        var i = 0
        var j = array.size - 1
        var tmp: Byte
        while (j > i) {
            tmp = array[j]
            array[j] = array[i]
            array[i] = tmp
            j--
            i++
        }
    }
}
