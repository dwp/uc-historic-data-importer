package app.batch

import org.apache.hadoop.conf.Configuration
import java.util.*


fun getEnv(envVar: String): String? {
    val value = System.getenv(envVar)
    return if (value.isBlank()) null else value
}


object Config {

    object Hbase {
        val config = Configuration().apply {
            // See also https://hbase.apache.org/book.html#hbase_default_configurations
            set("zookeeper.znode.parent", getEnv("K2HB_HBASE_ZOOKEEPER_PARENT") ?: "/hbase")
            set("hbase.zookeeper.quorum", getEnv("K2HB_HBASE_ZOOKEEPER_QUORUM") ?: "zookeeper")
            setInt("hbase.zookeeper.port", getEnv("K2HB_HBASE_ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
        }

        val dataTable = getEnv("K2HB_HBASE_DATA_TABLE") ?: "k2hb:ingest"
        val dataFamily = getEnv("K2HB_HBASE_DATA_FAMILY") ?: "topic"
        val topicTable = getEnv("K2HB_HBASE_TOPIC_TABLE") ?: "k2hb:ingest-topic"
        val topicFamily = getEnv("K2HB_HBASE_TOPIC_FAMILY") ?: "c"
        val topicQualifier = getEnv("K2HB_HBASE_TOPIC_QUALIFIER") ?: "msg"
    }

}