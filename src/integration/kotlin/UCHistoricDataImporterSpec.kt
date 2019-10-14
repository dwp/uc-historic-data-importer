import app.batch.HbaseClient
import io.kotlintest.shouldBe
import io.kotlintest.specs.StringSpec
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.log4j.Logger


class Kafka2HBaseSpec: StringSpec(){

    private val log = Logger.getLogger(Kafka2HBaseSpec::class.toString())

    init {
        "Number of records should match" {
           val hbase = HbaseClient.connect()
           val scan = Scan()
           val rowCount =  hbase.connection.getTable(TableName.valueOf("k2hb:ingest")).use { table ->
               val  scanner = table.getScanner(scan)
               val size = scanner.map { it }.size
               scanner.close()
               size
            }

            log.info("Row count : $rowCount")
            rowCount shouldBe 4
        }
    }

}
