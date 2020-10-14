package app.batch

import app.domain.HBaseRecord
import com.nhaarman.mockitokotlin2.*
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.NamespaceDescriptor
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Admin
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Table
import org.apache.hadoop.hbase.io.compress.Compression
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import java.nio.ByteBuffer

class HbaseClientTest {

    @Test
    fun testFilters() {
        val existsArray = (0..99).map { (it % 2) == 0 }.toBooleanArray()

        val payload = (0..99).map {
            HBaseRecord("$it".toByteArray(), "$it-body".toByteArray(), it.toLong() + 100)
        }

        val table  = mock<Table> {
            on { existsAll(any()) } doReturn existsArray
        }

        val hbaseConnection = mock<Connection> {
            on { getTable(any()) } doReturn table
        }

        val client = HbaseClient(hbaseConnection, "cf".toByteArray(), "record".toByteArray(), 2)

        val nonExistent = client.nonExistent("database:collection", payload)

        assertEquals(50, nonExistent.size)

        nonExistent.forEachIndexed { index, record ->
            assertEquals("${index * 2 + 1}", "${String(record.key)}")
        }
    }

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

        val hbaseClient = HbaseClient(connection, "cf".toByteArray(), "record".toByteArray(), 2)
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
        val hbaseRegionReplication = 3
        val hbaseClient = HbaseClient(connection, dataFamily, dataQualifier, hbaseRegionReplication)
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
                        compactionCompressionType = Compression.Algorithm.GZ
                        compressionType = Compression.Algorithm.GZ
                    })
            regionReplication = hbaseRegionReplication
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
        val hbaseRegionReplication = 3
        val hbaseClient = HbaseClient(connection, dataFamily, dataQualifier, hbaseRegionReplication)
        val newTableQualifier = "table2"
        val qualifiedTableName = "$namespace:$newTableQualifier"
        hbaseClient.ensureTable(qualifiedTableName)

        verify(adm, times(0)).createNamespace(any())

        val tableDescriptor = HTableDescriptor(TableName.valueOf(qualifiedTableName)).apply {
            addFamily(HColumnDescriptor(dataFamily)
                    .apply {
                        maxVersions = Int.MAX_VALUE
                        minVersions = 1
                        compactionCompressionType = Compression.Algorithm.GZ
                        compressionType = Compression.Algorithm.GZ
                    })
            setRegionReplication(hbaseRegionReplication)
        }

        verify(adm, times(1)).createTable(tableDescriptor)
    }
}
