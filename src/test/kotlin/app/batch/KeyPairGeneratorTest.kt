package app.batch

import app.configuration.HttpClientProvider
import app.domain.KeyPair
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.apache.hadoop.hbase.client.Connection
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@ActiveProfiles("awsS3")
@SpringBootTest
@TestPropertySource(properties = [
    "hbase.zookeeper.quorum=hbase",
    "aws.region=eu-west-1",
    "s3.bucket=not_set",
    "s3.prefix.folder=not_set",
    "data.key.service.url=phoney"
])
class KeyPairGeneratorTest {

    private val fileFormatRegex = "^[A-Za-z]*\\.[A-Za-z]*\\.[0-9]{4}\\.json\\.gz".toRegex()
    private val dataFileExtensionRegex = "\\.enc\$".toRegex()
    private val metadataFileExtensionRegex = "\\.encryption\\.json\$".toRegex()

    @Autowired
    private lateinit var keyPairGenerator: KeyPairGenerator
    @MockBean
    private lateinit var httpClientProvider: HttpClientProvider

    @MockBean
    private lateinit var connection: Connection

    @Test
    fun Should_Return_Keypairs_When_Given_List_Of_Keys_Match_Format() {
        val expectedKeyPairs = listOf(KeyPair("adb.collection.0000.json.gz.enc", "adb.collection.0000.json.gz.encryption.json"),
                KeyPair("cdb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.encryption.json"),
                KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.encryption.json"))

        val keys = listOf("adb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.enc",
            "adb.collection.0000.json.gz.encryption.json", "cdb.collection.0000.json.gz.encryption.json",
            "sdb.collection.0000.json.gz.encryption.json")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, "^[A-Za-z]*\\.[A-Za-z]*\\.[0-9]{4}\\.json\\.gz".toRegex(), "\\.enc\$".toRegex(), "\\.encryption\\.json\$".toRegex())

        assertEquals(expectedKeyPairs, actualKeyPairs)
    }

    @Test
    fun Should_Log_When_Given_Keys_Dont_Match_Format() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        val expectedKeyPairs = listOf(KeyPair("adb.collection.0000.json.gz.enc", "adb.collection.0000.json.gz.encryption.json"),
                KeyPair("cdb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.encryption.json"),
                KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.encryption.json"))

        val keys = listOf("adb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.enc",
            "adb.collection.0000.json.gz.encryption.json", "cdb.collection.0000.json.gz.encryption.json",
            "sdb.collection.0000.json.gz.encryption.json", "abc.json", "edf.json")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(8)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("2 key(s) that don't match the given file fileFormat $fileFormatRegex found"))
        assertTrue(formattedMessages.contains("Unmatched keys : abc.json,edf.json"))
        assertEquals(expectedKeyPairs, actualKeyPairs)

    }

    @Test
    fun Should_Log_When_Given_Keys_Dont_Match_File_Extensions() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        val expectedKeyPairs = listOf(KeyPair("adb.collection.0000.json.gz.enc", "adb.collection.0000.json.gz.encryption.json"),
                KeyPair("cdb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.encryption.json"),
                KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.encryption.json"))

        val keys = listOf("adb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.enc",
            "adb.collection.0000.json.gz.encryption.json", "cdb.collection.0000.json.gz.encryption.json",
            "sdb.collection.0000.json.gz.encryption.json", "adb.collection.0000.json.gz.pdf", "adb.collection.0000.json.gz.docx")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(8)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("adb.collection.0000.json.gz.pdf,adb.collection.0000.json.gz.docx matched file format but not matched neither data nor metadata file extension"))
        assertEquals(expectedKeyPairs, actualKeyPairs)

    }

    @Test
    fun Should_Log_When_Data_File_Doesnt_But_Metadata_File_Does_Exist() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)
        val expectedKeyPairs = listOf(KeyPair("cdb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.encryption.json"),
                KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.encryption.json"))

        val keys = listOf("cdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.enc",
            "adb.collection.0000.json.gz.encryption.json", "cdb.collection.0000.json.gz.encryption.json",
            "sdb.collection.0000.json.gz.encryption.json")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(9)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Data file not found for the metadata file adb.collection.0000.json.gz.encryption.json"))
        assertEquals(expectedKeyPairs, actualKeyPairs)

    }

    @Test(expected = RuntimeException::class)
    fun Should_Throw_Exception_When_Metadata_File_Doesnt_Exist() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()
        root.addAppender(mockAppender)

        val keys = listOf("adb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.enc",
            "cdb.collection.0000.json.gz.encryption.json",
            "sdb.collection.0000.json.gz.encryption.json")

        keyPairGenerator.generateKeyPairs(keys, fileFormatRegex, dataFileExtensionRegex, metadataFileExtensionRegex)

        val captor = argumentCaptor<ILoggingEvent>()
        verify(mockAppender, times(9)).doAppend(captor.capture())
        val formattedMessages = captor.allValues.map { it.formattedMessage }
        assertTrue(formattedMessages.contains("Metadata file not found for the data file adb.collection.0000.json.gz.enc"))
    }
}
