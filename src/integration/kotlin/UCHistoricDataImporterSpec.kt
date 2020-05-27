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
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.Logger
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.Assert.assertEquals
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.Reader
import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.security.Key
import java.security.Security
import java.text.SimpleDateFormat
import java.util.*
import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

@SpringBootTest(classes = [S3DummyConfiguration::class])
@ActiveProfiles("localS3")
class UCHistoricDataImporterSpec : FunSpec() {

    override fun listeners() = listOf(SpringListener)
    private val logger = Logger.getLogger(UCHistoricDataImporterSpec::class.toString())

    @Autowired
    private lateinit var s3Client: AmazonS3

    @Value("\${s3.manifest.bucket:manifestbucket}")
    private lateinit var s3BucketName: String

    @Value("\${s3.manifest.prefix.folder:test-manifest-exporter}")
    private lateinit var s3ManifestPrefixFolder: String

    private fun tableCount(connection: Connection, tableName: String) =
        connection.getTable(TableName.valueOf(tableName)).use { table ->
            val scanner = table.getScanner(Scan())
            val size = scanner.map { it }.size
            scanner.close()
            size
        }


    init {
        Security.addProvider(BouncyCastleProvider())

        test("Messages in Hbase should match the count 4") {
            val hbase = HbaseClient.connect()
            val t1count = tableCount(hbase.connection, "database_1:collection_1")
            t1count shouldBe 6
            val t2count = tableCount(hbase.connection, "database_1:collection")
            t2count shouldBe 3
            val t3count = tableCount(hbase.connection, "agent_core:agentToDo")
            t3count shouldBe 3
        }

        test("Messages in Hbase are decipherable") {
            val hbase = HbaseClient.connect()

            for (tableName in listOf("database_1:collection_1", "database_1:collection", "agent_core:agentToDo")) {
                hbase.connection.getTable(TableName.valueOf(tableName)).use { table ->
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
                                        //verify it is valid json:
                                        Gson().fromJson(decryptedDbObject, JsonObject::class.java)
                                    }
                                    catch (e: Exception) {
                                        fail("Decrypted db object should be parseable as json.")
                                    }
                                }
                                else {
                                    fail("No encrypted db object.")
                                }
                            }
                        }
                    }
                }

            }
        }

        test("Should match  manifest file count, content  generated in S3") {
            val removedTimestamp = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse("2012-03-04T21:43:56.000Z").time
            val archivedTimestamp = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse("2014-03-02T12:34:56.000Z").time
            val oid ="\$oid"
            val date ="\$date"
            val expected = """
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462000|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462001|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462002|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462003|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462004|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462005|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462006|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462007|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462008|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462009|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |agent_core.agentToDoArchive.0001-1|1543676462009|agent_core|agentToDo|IMPORT|HDI|"{""$oid"":""agent_core.agentToDoArchive.0001-1""}"|MONGO_IMPORT
            |"{""createdDateTime"":""2010-01-01T00:00:00.000+0000"",""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462009|agent_core|agentToDo|IMPORT|HDI|"{""createdDateTime"":{""$date"":""2010-01-01T00:00:00.000Z""},""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1330897436000|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_DELETE
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1393763696000|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_DELETE
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|1426854205183|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|315532800000|agent_core|agentToDo|IMPORT|HDI|"{""declarationId"":""agent_core.agentToDoArchive.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462000|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462001|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462002|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462003|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462004|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462005|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462006|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462007|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462008|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462009|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |database-1.collection-thirtyone.0001-1|1543676462009|database-1|collection|IMPORT|HDI|"{""$oid"":""database-1.collection-thirtyone.0001-1""}"|MONGO_IMPORT
            |"{""createdDateTime"":""2010-01-01T00:00:00.000+0000"",""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462009|database-1|collection|IMPORT|HDI|"{""createdDateTime"":{""$date"":""2010-01-01T00:00:00.000Z""},""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1330897436000|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_DELETE
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1393763696000|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_DELETE
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|1426854205183|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|315532800000|database-1|collection|IMPORT|HDI|"{""declarationId"":""database-1.collection-thirtyone.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462000|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462001|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462002|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462003|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462004|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462005|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462006|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462007|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462008|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462009|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |database-1.collection-1.0001-1|1543676462009|database-1|collection-1|IMPORT|HDI|"{""$oid"":""database-1.collection-1.0001-1""}"|MONGO_IMPORT
            |"{""createdDateTime"":""2010-01-01T00:00:00.000+0000"",""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1543676462009|database-1|collection-1|IMPORT|HDI|"{""createdDateTime"":{""$date"":""2010-01-01T00:00:00.000Z""},""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1330897436000|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_DELETE
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1393763696000|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_DELETE
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|1426854205183|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|315532800000|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0001-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462000|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462001|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462002|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462003|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462004|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462005|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462006|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462007|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462008|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462009|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |database-1.collection-1.0002-1|1543676462009|database-1|collection-1|IMPORT|HDI|"{""$oid"":""database-1.collection-1.0002-1""}"|MONGO_IMPORT
            |"{""createdDateTime"":""2010-01-01T00:00:00.000+0000"",""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1543676462009|database-1|collection-1|IMPORT|HDI|"{""createdDateTime"":{""$date"":""2010-01-01T00:00:00.000Z""},""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|$removedTimestamp|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_DELETE
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|$archivedTimestamp|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_DELETE
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|1426854205183|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            |"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|315532800000|database-1|collection-1|IMPORT|HDI|"{""declarationId"":""database-1.collection-1.0002-1"",""someId"":""RANDOM_GUID""}"|MONGO_IMPORT
            """.trimMargin()

            val summaries = s3Client.listObjectsV2(s3BucketName, s3ManifestPrefixFolder).objectSummaries
            val fileCount = summaries.size
            val list = summaries.map { summary ->
                val objectContent = s3Client.getObject(summary.bucketName, summary.key).objectContent
                val fileContent = BufferedReader(InputStreamReader(objectContent) as Reader).use { it.readText() }
                val noOfRecordsPerFile = fileContent.trim().split("\n").size
                assertEquals(16, noOfRecordsPerFile)
                fileContent.trim()
            }
            val joinedContent = list.joinToString("\n")
            assertEquals(4, fileCount)
            assertEquals(expected, joinedContent)

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
