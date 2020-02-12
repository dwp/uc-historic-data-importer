package app.batch

import app.utils.logging.JsonLoggerWrapper
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName

fun HbaseClient.migrate() {
    val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(HbaseClient::class.toString())

    connection.admin.use { admin ->
        val namespaces = admin.listNamespaceDescriptors().map { it.name }
        val dataTableName = TableName.valueOf(dataTable)
        val topicTableName = TableName.valueOf(topicTable)

        val missingNamespaces = setOf(
            dataTableName.namespaceAsString,
            topicTableName.namespaceAsString
        ).subtract(namespaces)

        // Create all namespaces not already in the list of namespaces
        for (namespace in missingNamespaces) {
            logger.info("Creating hbase namespace", "hbase_namespace", namespace)
            admin.createNamespace(NamespaceDescriptor.create(namespace).build())
        }

        // Create data table if not exists, else add the family
        val dataFamilyDescriptor = HColumnDescriptor(dataFamily).apply {
            maxVersions = Int.MAX_VALUE
            minVersions = 1
        }

        if (!admin.tableExists(dataTableName)) {
            logger.info("Creating hbase table", "hbase_table", dataTable)
            admin.createTable(HTableDescriptor(dataTableName).apply {
                addFamily(dataFamilyDescriptor)
            })
        }
        else if (!admin.getTableDescriptor(dataTableName).hasFamily(dataFamily)) {
            logger.info("Adding column family to hbase table", "hbase_column_family", "$dataFamily", "hbase_table", dataTable)
            admin.addColumn(dataTableName, dataFamilyDescriptor)
        }

        // Create topic table if not exists, else add the family
        val topicFamilyDescriptor = HColumnDescriptor(topicFamily)

        if (!admin.tableExists(topicTableName)) {
            logger.info("Creating hbase table ", "hbase_table", topicTable)
            admin.createTable(HTableDescriptor(topicTableName).apply {
                addFamily(topicFamilyDescriptor)
            })
        }
        else if (!admin.getTableDescriptor(topicTableName).hasFamily(topicFamily)) {
            logger.info("Adding column family to hbase table", "hbase_column_family", "$dataFamily", "hbase_table", dataTable)
            admin.addColumn(topicTableName, topicFamilyDescriptor)
        }
    }
}
