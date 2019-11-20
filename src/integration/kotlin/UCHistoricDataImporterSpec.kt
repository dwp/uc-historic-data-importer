import app.batch.HbaseClient
import app.configuration.S3DummyConfiguration
import com.amazonaws.services.s3.AmazonS3
import com.google.gson.Gson
import com.google.gson.JsonObject
import io.kotlintest.fail
import io.kotlintest.shouldBe
import io.kotlintest.specs.FunSpec
import io.kotlintest.spring.SpringListener
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.Logger
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.Assert
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.Reader
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.security.Key
import java.security.Security
import java.util.*
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

@SpringBootTest(classes = [S3DummyConfiguration::class])
class UCHistoricDataImporterSpec : FunSpec() {

    override fun listeners() = listOf(SpringListener)
    private val log = Logger.getLogger(UCHistoricDataImporterSpec::class.toString())

    @Autowired
    private lateinit var s3Client: AmazonS3

    @Value("\${s3.manifest.bucket:manifestbucket}")
    private lateinit var s3BucketName: String

    @Value("\${s3.manifest.prefix.folder:test-manifest-exporter}")
    private lateinit var s3ManifestPrefixFolder: String


    init {
        Security.addProvider(BouncyCastleProvider())

        test("Messages in Hbase should match the count 4") {
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

        test("Topics in Hbase should match the count 3") {
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

        test("Messages in Hbase are decipherable") {
            val hbase = HbaseClient.connect()
            hbase.connection.getTable(TableName.valueOf("k2hb:ingest")).use { table ->
                val scan = Scan()
                val scanner = table.getScanner(scan)
                scanner.forEach { result ->

                    val familyMap = result.noVersionMap

                    if (familyMap.size == 0) {
                        fail("No column family on the table.")
                    }

                    familyMap.forEach { (_, columnMap) ->
                        columnMap.forEach { (_, cell) ->
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
                                    println(jsonObject)
                                } catch (e: Exception) {
                                    fail("Decrypted db object should be parseable as json.")
                                }
                            } else {
                                fail("No encrypted db object.")
                            }
                        }
                    }
                }
            }
        }

        test("Test manifest generation in S3") {
            val expected = """
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462000,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462001,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462002,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462003,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462004,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462005,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462006,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462007,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462008,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-1.0002-1""}",1543676462009,database-1,collection-1,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462000,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462001,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462002,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462003,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462004,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462005,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462006,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462007,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462008,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0001-1""}",1543676462009,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462000,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462001,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462002,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462003,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462004,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462005,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462006,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462007,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462008,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-1.collection-2.0002-1""}",1543676462009,database-1,collection-2,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462000,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462001,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462002,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462003,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462004,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462005,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462006,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462007,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462008,database-2,collection-3,IMPORT
            |"{""someId"":""RANDOM_GUID"",""declarationId"":""database-2.collection-3.0001-1""}",1543676462009,database-2,collection-3,IMPORT
            
            """.trimMargin().trimIndent()

            val summaries = s3Client.listObjectsV2(s3BucketName, s3ManifestPrefixFolder).objectSummaries
            val list = summaries.map {
                val objectContent = s3Client.getObject(it.bucketName, it.key).objectContent
                BufferedReader(InputStreamReader(objectContent) as Reader?).use { it.readText() }
            }
            val joinedContent = list.joinToString("\n")
            log.info("all content $joinedContent")
            Assert.assertEquals(expected, joinedContent)
        }
    }

    fun decrypt(key: String, initializationVector: String, encrypted: String): String {
        val keySpec: Key = SecretKeySpec(Base64.getDecoder().decode(key), "AES")
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
