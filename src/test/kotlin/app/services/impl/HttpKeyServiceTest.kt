package app.services.impl

import app.batch.S3Reader
import app.configuration.HttpClientProvider
import app.configuration.S3Configuration
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import org.apache.hadoop.hbase.client.Connection
import org.apache.http.HttpEntity
import org.apache.http.StatusLine
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.CloseableHttpClient
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.BDDMockito.given
import org.mockito.Mockito.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream

@RunWith(SpringRunner::class)
@SpringBootTest
@TestPropertySource(properties = [
    "hbase.zookeeper.quorum=phoney",
    "data.key.service.url=phoney"
])
class HttpKeyServiceTest {

    @Before
    fun init() {
        this.keyService.clearCache()
        reset(this.httpClientProvider)
    }

    @Test
    fun testDecryptKeyOk() {
        val responseBody = """
            |{
            |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(200)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(ArgumentMatchers.any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
        Assert.assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)
    }

    @Test
    fun testDecryptKeyCaches() {
        val responseBody = """
            |{
            |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(200)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(ArgumentMatchers.any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
        //val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
        Assert.assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)
        keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
        verify(httpClient, times(1))
                .execute(ArgumentMatchers.any(HttpPost::class.java))
    }

    @Test(expected = DataKeyDecryptionException::class)
    fun testDecryptKeyBadKey() {
        try {
            val statusLine = mock(StatusLine::class.java)
            given(statusLine.statusCode).willReturn(400)
            val httpResponse = mock(CloseableHttpResponse::class.java)
            given(httpResponse.statusLine).willReturn(statusLine)
            val httpClient = mock(CloseableHttpClient::class.java)
            given(httpClient.execute(ArgumentMatchers.any(HttpPost::class.java))).willReturn(httpResponse)
            given(httpClientProvider.client()).willReturn(httpClient)
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
        } catch (e: DataKeyDecryptionException) {
            val expected = """Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with
                | keyEncryptionKeyId: '123'
                | data key service returned status code '400'""".trimMargin().replace("\n", "")
            Assert.assertEquals(expected, e.message)
            throw e;
        }
    }

    @Test(expected = DataKeyServiceUnavailableException::class)
    fun testDecryptKeyServerError() {
        try {
            val statusLine = mock(StatusLine::class.java)
            given(statusLine.statusCode).willReturn(503)
            val httpResponse = mock(CloseableHttpResponse::class.java)
            given(httpResponse.statusLine).willReturn(statusLine)
            val httpClient = mock(CloseableHttpClient::class.java)
            given(httpClient.execute(ArgumentMatchers.any(HttpPost::class.java))).willReturn(httpResponse)
            given(httpClientProvider.client()).willReturn(httpClient)
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
        } catch (e: DataKeyServiceUnavailableException) {
            val expected = """Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with
            | keyEncryptionKeyId: '123'
            | data key service returned status code '503'""".trimMargin().replace(Regex("\n"), "")
            Assert.assertEquals(expected, e.message)
            println(e.message)
            throw e
        }
    }

    @Autowired
    private lateinit var keyService: HttpKeyService

    @MockBean
    private lateinit var httpClientProvider: HttpClientProvider

    @MockBean
    private lateinit var s3reader: S3Reader

    @MockBean
    private lateinit var s3config: S3Configuration

    @MockBean
    private lateinit var connection: Connection

    companion object {
        val logger: Logger = LoggerFactory.getLogger(HttpKeyService::class.toString())
    }
}