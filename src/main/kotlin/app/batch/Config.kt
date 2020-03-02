package app.batch

import org.apache.hadoop.conf.Configuration

fun getEnv(envVar: String): String? {
    val value = System.getenv(envVar)
    return if (value.isNullOrEmpty()) null else value
}

val hBaseDataFamily = getEnv("K2HB_HBASE_DATA_FAMILY") ?: "cf"
val hBaseDataQualifier = getEnv("K2HB_HBASE_DATA_QUALIFIER") ?: "record"
val hBaseTopicTable = getEnv("K2HB_HBASE_TOPIC_TABLE") ?: "k2hb:ingest-topic"
val hBaseTopicFamily = getEnv("K2HB_HBASE_TOPIC_FAMILY") ?: "c"
val hBaseTopicQualifier = getEnv("K2HB_HBASE_TOPIC_QUALIFIER") ?: "msg"

object Config {

    object Hbase {
        val config = Configuration().apply {
            // See also https://hbase.apache.org/book.html#hbase_default_configurations
            set("zookeeper.znode.parent", getEnv("K2HB_HBASE_ZOOKEEPER_PARENT") ?: "/hbase")
            set("hbase.zookeeper.quorum", getEnv("K2HB_HBASE_ZOOKEEPER_QUORUM") ?: "zookeeper")
            setInt("hbase.zookeeper.port", getEnv("K2HB_HBASE_ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
            setInt("hbase.client.keyvalue.maxsize", getEnv("K2HB_HBASE_KEYVALUE_MAXSIZE")?.toIntOrNull() ?: 0)
        }

        val dataFamily = hBaseDataFamily
        val dataQualifier = hBaseDataQualifier
    }

}
