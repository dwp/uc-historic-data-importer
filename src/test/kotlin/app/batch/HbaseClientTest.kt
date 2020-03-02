package app.batch

import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.junit.Test

import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import java.nio.ByteBuffer

class HbaseClientTest {

    @Test
    fun ensureTableNotCreated() {
        val tableQualifier = "table"
        val namespace = "ns"
        val tableName = TableName.valueOf(ByteBuffer.wrap(namespace.toByteArray()), ByteBuffer.wrap(tableQualifier.toByteArray()))

        val namespaceDescriptor = mock<NamespaceDescriptor> {
            on { name } doReturn namespace
        }

        val adm = mock<Admin> {
            on { listNamespaceDescriptors() } doReturn arrayOf(namespaceDescriptor)
            on { listTableNames() } doReturn arrayOf(tableName)
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val hbaseClient = HbaseClient(connection, "cf".toByteArray(), "record".toByteArray())
        hbaseClient.ensureTable("$namespace:$tableQualifier")
        verify(adm, times(0)).createNamespace(any())
        verify(adm, times(0)).createTable(any())
    }

    @Test
    fun ensureNamespaceCreatedAndTable() {
        val tableQualifier = "table"
        val namespace = "ns"
        val tableName = TableName.valueOf(ByteBuffer.wrap(namespace.toByteArray()), ByteBuffer.wrap(tableQualifier.toByteArray()))

        val namespaceDescriptor = mock<NamespaceDescriptor> {
            on { name } doReturn namespace
        }

        val adm = mock<Admin> {
            on { listNamespaceDescriptors() } doReturn arrayOf(namespaceDescriptor)
            on { listTableNames() } doReturn arrayOf(tableName)
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val dataFamily = "cf".toByteArray()
        val dataQualifier = "record".toByteArray()
        val hbaseClient = HbaseClient(connection, dataFamily, dataQualifier)
        val newNamespace = "ns2"
        val newTableQualifier = "table2"
        val qualifiedTableName = "$newNamespace:$newTableQualifier"
        hbaseClient.ensureTable(qualifiedTableName)

        verify(adm, times(1)).createNamespace(any())

        val tableDescriptor = HTableDescriptor(TableName.valueOf(qualifiedTableName)).apply {
            addFamily(HColumnDescriptor(dataFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                    })
        }

        verify(adm, times(1)).createTable(tableDescriptor)
    }

    @Test
    fun ensureTableCreatedButNotNamespace() {
        val tableQualifier = "table"
        val namespace = "ns"
        val tableName = TableName.valueOf(ByteBuffer.wrap(namespace.toByteArray()), ByteBuffer.wrap(tableQualifier.toByteArray()))

        val namespaceDescriptor = mock<NamespaceDescriptor> {
            on { name } doReturn namespace
        }

        val adm = mock<Admin> {
            on { listNamespaceDescriptors() } doReturn arrayOf(namespaceDescriptor)
            on { listTableNames() } doReturn arrayOf(tableName)
        }

        val connection = mock<Connection> {
            on { admin } doReturn adm
        }

        val dataFamily = "cf".toByteArray()
        val dataQualifier = "record".toByteArray()
        val hbaseClient = HbaseClient(connection, dataFamily, dataQualifier)
        val newTableQualifier = "table2"
        val qualifiedTableName = "$namespace:$newTableQualifier"
        hbaseClient.ensureTable(qualifiedTableName)

        verify(adm, times(0)).createNamespace(any())

        val tableDescriptor = HTableDescriptor(TableName.valueOf(qualifiedTableName)).apply {
            addFamily(HColumnDescriptor(dataFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                    })
        }

        verify(adm, times(1)).createTable(tableDescriptor)
    }
}
