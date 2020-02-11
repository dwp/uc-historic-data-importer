package app.batch

import app.utils.logging.logInfo
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.slf4j.LoggerFactory

fun HbaseClient.migrate() {
    val logger = LoggerFactory.getLogger(HbaseClient::class.toString())

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
            logInfo(logger, "Creating namespace '$namespace'")
            admin.createNamespace(NamespaceDescriptor.create(namespace).build())
        }

        // Create data table if not exists, else add the family
        val dataFamilyDescriptor = HColumnDescriptor(dataFamily).apply {
            maxVersions = Int.MAX_VALUE
            minVersions = 1
        }

        if (!admin.tableExists(dataTableName)) {
            logInfo(logger, "Creating table '$dataTable'")
            admin.createTable(HTableDescriptor(dataTableName).apply {
                addFamily(dataFamilyDescriptor)
            })
        }
        else if (!admin.getTableDescriptor(dataTableName).hasFamily(dataFamily)) {
            logInfo(logger, "Adding column family '$dataFamily' to table '$dataTable'")
            admin.addColumn(dataTableName, dataFamilyDescriptor)
        }

        // Create topic table if not exists, else add the family
        val topicFamilyDescriptor = HColumnDescriptor(topicFamily)

        if (!admin.tableExists(topicTableName)) {
            logInfo(logger, "Creating table '$topicTable'")
            admin.createTable(HTableDescriptor(topicTableName).apply {
                addFamily(topicFamilyDescriptor)
            })
        }
        else if (!admin.getTableDescriptor(topicTableName).hasFamily(topicFamily)) {
            logInfo(logger, "Adding column family '$topicFamily' to table '$topicTable'")
            admin.addColumn(topicTableName, topicFamilyDescriptor)
        }
    }
}
