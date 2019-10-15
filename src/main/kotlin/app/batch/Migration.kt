package app.batch

import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.log4j.Logger

fun HbaseClient.migrate() {
    val logger = Logger.getLogger(HbaseClient::class.java)

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
            logger.info("Creating namespace '$namespace'")
            admin.createNamespace(NamespaceDescriptor.create(namespace).build())
        }

        // Create data table if not exists, else add the family
        val dataFamilyDescriptor = HColumnDescriptor(dataFamily).apply {
            maxVersions = Int.MAX_VALUE
            minVersions = 1
        }

        initTable(admin, dataTableName, dataFamilyDescriptor)
        val topicFamilyDescriptor = HColumnDescriptor(topicFamily)
        initTable(admin, topicTableName, topicFamilyDescriptor)
    }
}

private fun HbaseClient.initTable(admin: Admin, dataTableName: TableName?, dataFamilyDescriptor: HColumnDescriptor) {
    val logger = Logger.getLogger(HbaseClient::class.java)
    if (!admin.tableExists(dataTableName)) {
        logger.info("Creating table '$dataTable'")
        admin.createTable(HTableDescriptor(dataTableName).apply {
            addFamily(dataFamilyDescriptor)
        })
    } else if (!admin.getTableDescriptor(dataTableName).hasFamily(dataFamily)) {
        logger.info("Adding column family '$dataFamily' to table '$dataTable'")
        admin.addColumn(dataTableName, dataFamilyDescriptor)
    }
}