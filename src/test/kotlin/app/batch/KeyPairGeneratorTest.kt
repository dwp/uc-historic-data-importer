package app.batch

import app.configuration.HttpClientProvider
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.Appender
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import com.nhaarman.mockitokotlin2.*
import org.slf4j.Logger
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
    "s3.prefix.folder=not_set"
])
class KeyPairGeneratorTest {

   /* @Test
    fun Should_Provide_Keypairs_When_Given_List_Of_Keys_And_Format() {
        val expectedKeyPairs = listOf<KeyPair>(KeyPair("adb.collection.0000.json.gz.enc", "adb.collection.0000.json.gz.encryption.json"),
            KeyPair("cdb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.encryption.json"),
            KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.encryption.json"))

        val keys = listOf("adb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.enc",
            "adb.collection.0000.json.gz.encryption.json", "cdb.collection.0000.json.gz.encryption.json",
            "sdb.collection.0000.json.gz.encryption.json")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, "^[A-Za-z]*\\.[A-Za-z]*\\.[0-9]{4}\\.json\\.gz".toRegex(), "\\.enc\$".toRegex(), "\\.encryption\\.json\$".toRegex())

        assertEquals(expectedKeyPairs, actualKeyPairs)
    }*/

    @Test
    fun Should_Log_Keys_When_Given_Keys_That_Dont_Match_Format() {

        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
        val mockAppender: Appender<ILoggingEvent> = mock()

        whenever(mockAppender.name).thenReturn("MOCK")
        root.addAppender(mockAppender)



        val expectedKeyPairs = listOf<KeyPair>(KeyPair("adb.collection.0000.json.gz.enc", "adb.collection.0000.json.gz.encryption.json"),
            KeyPair("cdb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.encryption.json"),
            KeyPair("sdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.encryption.json"))

        val keys = listOf("adb.collection.0000.json.gz.enc", "cdb.collection.0000.json.gz.enc", "sdb.collection.0000.json.gz.enc",
            "adb.collection.0000.json.gz.encryption.json", "cdb.collection.0000.json.gz.encryption.json",
            "sdb.collection.0000.json.gz.encryption.json", "abc.json", "adb.collection.0000.json.gz.pdf","adb.collection.0000.json.gz.docx")

        val actualKeyPairs = keyPairGenerator.generateKeyPairs(keys, "^[A-Za-z]*\\.[A-Za-z]*\\.[0-9]{4}\\.json\\.gz".toRegex(), "\\.enc\$".toRegex(), "\\.encryption\\.json\$".toRegex())

        assertEquals(expectedKeyPairs, actualKeyPairs)
        argumentCaptor<ILoggingEvent>().apply {
            verify(mockAppender, times(8)).doAppend(capture())

            println(allValues)
        }

    }


    @Autowired
    private lateinit var keyPairGenerator: KeyPairGenerator

    @MockBean
    private lateinit var httpClientProvider: HttpClientProvider

    companion object {
        val logger: Logger = LoggerFactory.getLogger(KeyPairGeneratorTest::class.toString())
    }
}
