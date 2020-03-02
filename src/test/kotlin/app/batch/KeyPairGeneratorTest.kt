package app.batch

import app.domain.KeyPair
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.slf4j.LoggerFactory

class KeyPairGeneratorTest {

    private val fileFormatRegex = """[\w-]+\.[\w-]+\.[0-9]+\.json""".toRegex()
    private val dataFileExtensionRegex = """\.gz\.enc$""".toRegex()
    private val metadataFileExtensionRegex = """\.encryption\.json$""".toRegex()
    private val keyPairGenerator: KeyPairGenerator = KeyPairGenerator()

    @Test
    fun should_Return_Keypairs_When_Given_List_Of_Keys_Match_Format() {
        val expectedKeyPairs = listOf(
                KeyPair("adb.collection.0000.json.gz.enc", "adb.collection.0000.json.encryption.json"),
                KeyPair("cdb.Collection_1.0000.json.gz.enc", "cdb.Collection_1.0000.json.encryption.json"),
                KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.encryption.json"))

        val keys = listOf(
                "adb.collection.0000.json.gz.enc",
                "cdb.Collection_1.0000.json.gz.enc",
                "sdb.collection.0000.json.gz.enc",
                "adb.collection.0000.json.encryption.json",
                "cdb.Collection_1.0000.json.encryption.json",
                "sdb.collection.0000.json.encryption.json")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        assertEquals(expectedKeyPairs, actualKeyPairs)
    }

    @Test
    fun should_Log_When_Given_Keys_Dont_Match_Format() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val expectedKeyPairs = listOf(
                KeyPair("adb.collection.0000.json.gz.enc", "adb.collection.0000.json.encryption.json"),
                KeyPair("cdb.Collection_1.0000.json.gz.enc", "cdb.Collection_1.0000.json.encryption.json"),
                KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.encryption.json"))

        val keys = listOf(
                "adb.collection.0000.json.gz.enc",
                "cdb.Collection_1.0000.json.gz.enc",
                "sdb.collection.0000.json.gz.enc",
                "adb.collection.0000.json.encryption.json",
                "cdb.Collection_1.0000.json.encryption.json",
                "sdb.collection.0000.json.encryption.json",
                "abc.json",
                "edf.json")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(4)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        val expected = "Found unmatched keys not matching regex\", \"unmatched_count\":\"2\", \"file_format\":\"[\\\\w-]+\\\\.[\\\\w-]+\\\\.[0-9]+\\\\.json\", \"unmatched_keys\":\"abc.json, edf.json"
        println(expected)
        assertTrue(formattedMessages.contains(expected))
        assertEquals(expectedKeyPairs, actualKeyPairs)
    }

    @Test
    fun should_Log_When_Given_Keys_Dont_Match_File_Extensions() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        val expectedKeyPairs = listOf(
                KeyPair("adb.collection.0000.json.gz.enc", "adb.collection.0000.json.encryption.json"),
                KeyPair("cdb.collection.0000.json.gz.enc", "cdb.collection.0000.json.encryption.json"),
                KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.encryption.json"))

        val keys = listOf(
                "adb.collection.0000.json.gz.enc",
                "cdb.collection.0000.json.gz.enc",
                "sdb.collection.0000.json.gz.enc",
                "adb.collection.0000.json.encryption.json",
                "cdb.collection.0000.json.encryption.json",
                "sdb.collection.0000.json.encryption.json",
                "adb.collection.0000.json.gz.pdf",
                "adb.collection.0000.json.gz.docx")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(4)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Found file(s) that matched format but neither data or metadata file extensions\", \"bad_files\":\"adb.collection.0000.json.gz.pdf, adb.collection.0000.json.gz.docx"))
        assertEquals(expectedKeyPairs, actualKeyPairs)
    }

    @Test
    fun should_Log_When_Data_File_Doesnt_But_Metadata_File_Does_Exist() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        val expectedKeyPairs = listOf(
                KeyPair("cdb.collection.0000.json.gz.enc", "cdb.collection.0000.json.encryption.json"),
                KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.encryption.json"))

        val keys = listOf(
                "cdb.collection.0000.json.gz.enc",
                "sdb.collection.0000.json.gz.enc",
                "adb.collection.0000.json.encryption.json",
                "cdb.collection.0000.json.encryption.json",
                "sdb.collection.0000.json.encryption.json")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(4)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Data file not found for metadata file\", \"metadata_file\":\"adb.collection.0000.json.encryption.json"))
        assertEquals(expectedKeyPairs, actualKeyPairs)
    }

    @Test(expected = RuntimeException::class)
    fun should_Throw_Exception_When_Metadata_File_Doesnt_Exist() {
        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val keys = listOf(
                "adb.collection.0000.json.gz.enc",
                "cdb.collection.0000.json.gz.enc",
                "sdb.collection.0000.json.gz.enc",
                "cdb.collection.0000.json.encryption.json",
                "sdb.collection.0000.json.encryption.json")

        keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(9)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Metadata file not found for the data file adb.collection.0000.json.gz.enc"))
    }
}
