package app.batch

import app.domain.DecompressedStream
import com.nhaarman.mockitokotlin2.mock
import org.apache.hadoop.hbase.client.Connection
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream

@RunWith(SpringRunner::class)
@ActiveProfiles("httpDataKeyService", "awsS3")
@SpringBootTest
@TestPropertySource(properties = [
    "hbase.zookeeper.quorum=hbase",
    "aws.region=eu-west-1",
    "s3.bucket=bucket1",
    "s3.prefix.folder=test/output/",
    "s3.key.regex=([A-Za-z]*\\.[A-Za-z]*\\.[0-9]{4}\\.json\\.gz)",
    "s3.data.key.extension=\\.enc$",
    "s3.metadata.key.extension=\\.encryption\\.json$",
    "data.key.service.url=phoney"
])
class HbaseWriterTest {

    @Autowired
    private lateinit var s3Reader: S3Reader
    val invalidJson = """
            {"_id":{"declarationId":"47a4fad9-49af-4cb2-91b0-0056e2ac0eef"},
            "type":"addressDeclaration"
        """.trimIndent()
    val validJson1 = """
            {"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},
            "type":"addressDeclaration"
            }
        """.trimIndent()
    val validJson2 = """
            {"_id":{"declarationId":"47a4fad9-49af-4cb2-91b0-0056e2ac0eef"},
            "type":"addressDeclaration"
            }
        """.trimIndent()

    @Test
    fun should_Log_And_Continue_When_DBObject_IsNot_Valid_Json() {

        val mockHbaseConnection = mock<Connection>()
        val hbaseWriter = HBaseWriter(mockHbaseConnection)
        val data1 = listOf(invalidJson, validJson1)
        val data2 = listOf(validJson2, invalidJson)
        val inputStreams = mutableListOf(getInputStream(data1, "file1"),getInputStream(data2, "file2") )
        hbaseWriter.write(inputStreams)


    }

    private fun getInputStream(data1: List<String>, fileName: String) : DecompressedStream {
        val baos = ByteArrayOutputStream()
        data1.forEach {
            baos.write(it.toByteArray())
        }
        val inputStream = ByteArrayInputStream(baos.toByteArray())
        return DecompressedStream(inputStream, fileName)
    }
}
