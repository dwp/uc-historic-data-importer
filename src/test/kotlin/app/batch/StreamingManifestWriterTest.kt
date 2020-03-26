package app.batch

import app.domain.ManifestRecord
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.amazonaws.services.s3.AmazonS3
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.slf4j.LoggerFactory
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
            on { length() } doReturn 10
        }
        val manifestBucket = "manifestBucket"
        val manifestPrefix = "manifestPrefix"
        val manifestWriter = StreamingManifestWriter()
        manifestWriter.sendManifest(s3, manifestFile, manifestBucket, manifestPrefix, 10)
        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(11)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }

        for (i in 1..10) {
            assertTrue(formattedMessages.contains("Failed to write manifest\", \"attempt_number\":\"$i\", \"max_attempts\":\"10\", \"error_message\":\"null"))
        }
        assertTrue(formattedMessages.contains("Failed to write manifest after max attempts - giving up\", \"manifest_file\":\"manifest.csv\", \"max_attempts\":\"10"))
    }

}
