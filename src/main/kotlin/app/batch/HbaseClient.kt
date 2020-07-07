package app.batch

import app.domain.HBaseRecord
import app.utils.logging.JsonLoggerWrapper
import org.apache.hadoop.hbase.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.io.compress.Compression

open class HbaseClient(
        val connection: Connection,
        private val dataFamily: ByteArray,
        private val dataQualifier: ByteArray,
        private val hbaseRegionReplication: Int
) {
    companion object {
        fun connect() = HbaseClient(
            ConnectionFactory.createConnection(HBaseConfiguration.create(Config.Hbase.config)),
            Config.Hbase.dataFamily.toByteArray(),
            Config.Hbase.dataQualifier.toByteArray(),
            Config.Hbase.regionReplication
        )

        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(HbaseClient::class.toString())
    }

    open fun putBatch(tableName: String, inserts: List<HBaseRecord>) {
        if (inserts.isNotEmpty()) {
            connection.getTable(TableName.valueOf(tableName)).use {
                it.put(inserts.map { record ->
                    Put(record.key).apply {
                        this.addColumn(dataFamily, dataQualifier, record.version, record.body)
                        this.durability = Durability.SKIP_WAL
                    }
                })
            }
        }
    }

    open fun exists(tableName: String, id: ByteArray, timestamp: Long) =
        connection.getTable(TableName.valueOf(tableName)).use { table ->
            table.exists(Get(id).apply {
                addColumn(dataFamily, dataQualifier)
                setTimeStamp(timestamp)
            })
        }

    @Synchronized
    fun ensureTable(tableName: String) {
        val dataTableName = TableName.valueOf(tableName)
        val namespace = dataTableName.namespaceAsString

        if (!namespaces.contains(namespace)) {
            try {
                logger.info("Creating namespace '$namespace'.")
                connection.admin.createNamespace(NamespaceDescriptor.create(namespace).build())
            }
            catch (e: NamespaceExistException) {
                logger.info("'$namespace' already exists, probably created by another process")
            }
            finally {
                namespaces[namespace] = true
            }
        }

        if (!tables.contains(tableName)) {
            try {
                logger.info("Creating table '$dataTableName'.")
                connection.admin.createTable(HTableDescriptor(dataTableName).apply {
                    addFamily(HColumnDescriptor(dataFamily)
                        .apply {
                            maxVersions = Int.MAX_VALUE
                            minVersions = 1
                            compressionType = Compression.Algorithm.GZ
                            compactionCompressionType = Compression.Algorithm.GZ
                        })
                    setRegionReplication(hbaseRegionReplication)
                })
            }
            catch (e: Exception) {
                logger.info("'$tableName' already exists, probably created by another process")
            }
            finally {
                tables[tableName] = true
            }
        }
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
