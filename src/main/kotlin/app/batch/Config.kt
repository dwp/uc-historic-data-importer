package app.batch

import app.utils.logging.*
import org.apache.hadoop.conf.Configuration

fun getEnv(envVar: String): String? {
    val value = System.getenv(envVar)
    return if (value.isNullOrEmpty()) null else value
}


object Config {

    object Hbase {
        val config = Configuration().apply {
            // See also https://hbase.apache.org/book.html#hbase_default_configurations
            set("zookeeper.znode.parent", getEnv("K2HB_HBASE_ZOOKEEPER_PARENT") ?: "/hbase")
            set("hbase.zookeeper.quorum", getEnv("K2HB_HBASE_ZOOKEEPER_QUORUM") ?: "zookeeper")
            setInt("hbase.zookeeper.port", getEnv("K2HB_HBASE_ZOOKEEPER_PORT")?.toIntOrNull() ?: 2181)
            setInt("hbase.client.keyvalue.maxsize", getEnv("K2HB_HBASE_KEYVALUE_MAXSIZE")?.toIntOrNull() ?: 0)
        }

        val dataTable = data_table
        val dataFamily = data_family
        val topicTable = topic_table
        val topicFamily = topic_family
        val topicQualifier = topic_qualifier
    }

}
