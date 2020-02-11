package app.batch

import app.domain.HBaseRecord
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory

open class HbaseClient (
    val connection: Connection,
    val dataFamily: ByteArray,
    val dataQualifier: ByteArray
) {
    companion object {
        fun connect() = HbaseClient(
            ConnectionFactory.createConnection(HBaseConfiguration.create(Config.Hbase.config)),
                Config.Hbase.dataFamily.toByteArray(),
                Config.Hbase.dataQualifier.toByteArray()
        )

        val logger: Logger = LoggerFactory.getLogger(HbaseClient::class.java)
    }

    open fun putBatch(tableName: String, inserts: List<HBaseRecord>) {
        if (inserts.isNotEmpty()) {
            table(tableName).use {
                it.put(inserts.map { record ->
                    Put(record.key).apply {
                        this.addColumn(dataFamily, dataQualifier, record.version, record.body)
                    }
                })
            }
        }
    }

    private fun table(tableName: String): Table {
        val dataTableName = TableName.valueOf(tableName)
        val namespace = dataTableName.namespaceAsString

        if (!namespaces.contains(namespace)) {
            logger.info("Creating namespace '$namespace'.")
            connection.admin.createNamespace(NamespaceDescriptor.create(namespace).build())
            namespaces[namespace] = true
        }

        if (!tables.contains(tableName)) {
            logger.info("Creating table '$dataTableName'.")
            connection.admin.createTable(HTableDescriptor(dataTableName).apply {
                addFamily(HColumnDescriptor(dataFamily)
                            .apply {
                                maxVersions = Int.MAX_VALUE
                                minVersions = 1
                            })
            })
        }

        return connection.getTable(TableName.valueOf(tableName))
    }

    private val namespaces by lazy {
        val extantNamespaces = mutableMapOf<String, Boolean>()

        connection.admin.listNamespaceDescriptors()
                .forEach {
                    extantNamespaces[it.name] = true
                }

        extantNamespaces
    }

    private val tables by lazy {
        val names = mutableMapOf<String, Boolean>()

        connection.admin.listTableNames().forEach {
            names[it.nameAsString] = true
        }

        names
    }

    fun close() = connection.close()
}
