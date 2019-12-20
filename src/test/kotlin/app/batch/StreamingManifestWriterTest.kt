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
import java.io.BufferedInputStream
import java.io.ByteArrayInputStream
import java.io.File

class StreamingManifestWriterTest {


    @Test
    fun testEscapedCSV() {
        val manifestWriter = StreamingManifestWriter()
        val manifestRecord = ManifestRecord(""""_id":{"declarationId": "1234567890"}""", 100000000, "dbwithcomma,", "collectionwithdoublequote\"", "IMPORT", "HDI")
        val actual = manifestWriter.csv(manifestRecord)
        val expected = "\"\"\"_id\"\":{\"\"declarationId\"\": \"\"1234567890\"\"}\",100000000,\"dbwithcomma,\",\"collectionwithdoublequote\"\"\",IMPORT,HDI\n"
        assertEquals(expected, actual)
    }

    @Test
    fun testManifestFileMetadataGeneration() {
        val manifestWriter = StreamingManifestWriter()
        val manifestFileName = "test-manifest-exporter/db.core.addressDeclaration.csv"
        val actual = manifestWriter.manifestMetadata(manifestFileName, 1024)
        assertEquals("text/plain", actual.contentType)
        assertEquals(manifestFileName, actual.userMetadata.get("x-amz-meta-title"))
        assertEquals(1024, actual.contentLength)

    }

    @Test
    fun testManifestLogsException() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        val s3 = mock<AmazonS3>()
        val manifestFile = mock<File> {
            on { name } doReturn "manifest.csv"
            on { toString() } doReturn "manifest.csv"
        }
        val manifestBucket = "manifestBucket"
        val manifestPrefix = "manifestPrefix"
        val manifestWriter = StreamingManifestWriter()
        manifestWriter.sendManifest(s3, manifestFile, manifestBucket, manifestPrefix, 10)
        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(11)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        for (i in 1..10) {
            Assert.assertTrue(formattedMessages.contains("Failed to write manifest 'manifest.csv' on attempt $i/10: 'null'"))
        }
        Assert.assertTrue(formattedMessages.contains("Failed to write manifest 'manifest.csv' after 10 attempts, giving up."))
    }


    companion object {
        val logger: Logger = LoggerFactory.getLogger(StreamingManifestWriterTest::class.toString())
    }

}
