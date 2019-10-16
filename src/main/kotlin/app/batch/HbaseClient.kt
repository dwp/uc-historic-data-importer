package app.batch

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Put

open class HbaseClient(
        val connection: Connection,
        val dataTable: String,
        val dataFamily: ByteArray,
        val topicTable: String,
        val topicFamily: ByteArray,
        val topicQualifier: ByteArray
) {
    companion object {
        fun connect() = HbaseClient(
                ConnectionFactory.createConnection(HBaseConfiguration.create(Config.Hbase.config)),
                Config.Hbase.dataTable,
                Config.Hbase.dataFamily.toByteArray(),
                Config.Hbase.topicTable,
                Config.Hbase.topicFamily.toByteArray(),
                Config.Hbase.topicQualifier.toByteArray()
        )
    }

    open fun putVersion(topic: ByteArray, key: ByteArray, body: ByteArray, version: Long) {
        connection.getTable(TableName.valueOf(dataTable)).use { table ->
            table.put(Put(key).apply {
                this.addColumn(
                        dataFamily,
                        topic,
                        version,
                        body
                )
            })
        }

        connection.getTable(TableName.valueOf(topicTable)).use { table ->
            table.increment(Increment(topic).apply {
                addColumn(
                        topicFamily,
                        topicQualifier,
                        1
                )
            })
        }
    }

    fun close() = connection.close()
}
