package app.batch

import app.domain.ManifestRecord
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectRequest
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.anyInt
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [ManifestWriter::class])
@TestPropertySource(properties = [
    "aws.region=eu-west-1",
    "s3.manifest.prefix.folder=test-manifest-exporter",
    "s3.manifest.bucket=manifestbucket"
])
class ManifestWriterTest {


    @Before
    fun setUp() {
        Mockito.reset(s3Client)
    }

    @Test
    fun testGenerateManifestFileFormat() {
        val expected = "test-manifest-exporter/db.core.addressDeclaration-000004.csv"
        val actual = manifestWriter.generateManifestFileFormat("db.core.addressDeclaration", 4)
        assertEquals(expected, actual)
    }

    @Test
    fun testGenerateEscapedCSV() {
        val manifestRecord1 = ManifestRecord("\"_id\":{\"declarationId\": \"1234567890\"}", 100000000, "dbwithcomma,", "collectionwithdoublequote\"", "IMPORT", "HDI")
        val manifestRecord2 = ManifestRecord("id2", 200000000, "db2", "collection2", "EXPORT", "@V4")
        val list = mutableListOf<ManifestRecord>()
        list.add(manifestRecord1)
        list.add(manifestRecord2)
        val actual = manifestWriter.generateEscapedCSV(list)
        val expected = "\"\"\"_id\"\":{\"\"declarationId\"\": \"\"1234567890\"\"}\",100000000,\"dbwithcomma,\",\"collectionwithdoublequote\"\"\",IMPORT,HDI\n" +
            "id2,200000000,db2,collection2,EXPORT,@V4"
        assertEquals(expected, actual)
    }

    @Test
    fun testManifestFileMetadataGeneration() {
        val manifestFileName = "test-manifest-exporter/db.core.addressDeclaration.csv"
        val actual = manifestWriter.generateManifestFileMetadata(manifestFileName, 1024)
        assertEquals("binary/octetstream", actual.contentType)
        assertEquals(manifestFileName, actual.userMetadata.get("x-amz-meta-title"))
        assertEquals(1024, actual.contentLength)

    }

    @Test
    fun testManifest() {
        val manifestRecord1 = ManifestRecord("id1", 100000000, "db1", "collection1", "IMPORT", "HDI")
        val manifestRecord2 = ManifestRecord("id2", 200000000, "db2", "collection2", "IMPORT", "HDI")
        val list = mutableListOf<ManifestRecord>()
        list.add(manifestRecord1)
        list.add(manifestRecord2)
        manifestWriter.generateManifest(list, "db", "collection", "000001")
        Mockito.verify(s3Client, Mockito.times(1))
            .putObject(ArgumentMatchers.any(PutObjectRequest::class.java))
    }

    @Test
    fun testManifestLogsException() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        doThrow(RuntimeException()).whenever(manifestWriter).generateManifestFileFormat(anyString(), anyInt())
        val manifestRecord = ManifestRecord("id1", 100000000, "db1", "collection1", "IMPORT", "HDI")
        val list = mutableListOf<ManifestRecord>()
        list.add(manifestRecord)
        manifestWriter.generateManifest(list, "db", "collection", "000001")
        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(1)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        Assert.assertTrue(formattedMessages.contains("Exception while writing ids of db: 'db1', collection: 'collection1' to manifest files in S3"))
    }

    @SpyBean
    private lateinit var manifestWriter: ManifestWriter

    @MockBean
    private lateinit var s3Client: AmazonS3

    companion object {
        val logger: Logger = LoggerFactory.getLogger(ManifestWriterTest::class.toString())
    }

}
