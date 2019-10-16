import app.batch.HbaseClient
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.Logger


class Kafka2HBaseSpec : StringSpec() {

    private val log = Logger.getLogger(Kafka2HBaseSpec::class.toString())

    init {
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
    }

}
