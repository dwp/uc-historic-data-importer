package app.services.impl

import app.configuration.HttpClientProvider
import app.domain.DataKeyResult
import app.exceptions.DataKeyDecryptionException
import app.exceptions.DataKeyServiceUnavailableException
import app.services.KeyService
import app.utils.UUIDGenerator
import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.firstValue
import com.nhaarman.mockitokotlin2.whenever
import org.apache.http.HttpEntity
import org.apache.http.StatusLine
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.CloseableHttpClient
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.BDDMockito.given
import org.mockito.Mockito.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.ByteArrayInputStream

@EnableRetry
@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HttpKeyService::class])
@TestPropertySource(properties = [
    "data.key.service.url=http://dummydks",
    "phoney.key=phoney-value"
])
class HttpKeyServiceTest {

    @Autowired
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var httpClientProvider: HttpClientProvider

    @MockBean
    private lateinit var uuidGenerator: UUIDGenerator

    companion object {
        private var dksCorrelationId = 0

        private fun nextDksCorrelationId(): String {
            return "dks-id-${++dksCorrelationId}"
        }
    }

    @Before
    fun init() {
        this.keyService.clearCache()
        reset(this.httpClientProvider)
        reset(this.uuidGenerator)
    }

    @Test
    fun testBatchDataKey_WillCallClientOnce_AndReturnKey() {
        val responseBody = """
            |{
            |    "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |    "plaintextDataKey": "PLAINTEXT_DATAKEY",
            |    "ciphertextDataKey": "CIPHERTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(201)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpGet::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.batchDataKey()

        val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
        assertEquals(expectedResult, dataKeyResult)
        val argumentCaptor = ArgumentCaptor.forClass(HttpGet::class.java)
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("http://dummydks/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())

    }

    @Test
    fun testBatchDataKey_ServerError_ThrowsException_AndWillRetry() {
        val httpClient = mock(CloseableHttpClient::class.java)
        val statusLine = mock(StatusLine::class.java)
        //val entity = mock(HttpEntity::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpClient.execute(any(HttpGet::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.batchDataKey()
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Calling batchDataKey: dks_url: 'http://dummydks/datakey', dks_correlation_id: '$dksCallId' returned status_code '503'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpGet::class.java)
            verify(httpClient, times(HttpKeyService.maxAttempts)).execute(argumentCaptor.capture())
            assertEquals("http://dummydks/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    @Throws(DataKeyServiceUnavailableException::class)
    fun testBatchDataKey_UnknownHttpError_ThrowsException_AndWillRetry() {
        val statusLine = mock(StatusLine::class.java)
        //val entity = mock(HttpEntity::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpGet::class.java))).willThrow(RuntimeException("Boom!"))
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.batchDataKey()
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Error contacting data key service: 'java.lang.RuntimeException: Boom!', dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpGet::class.java)
            verify(httpClient, times(HttpKeyService.maxAttempts)).execute(argumentCaptor.capture())
            assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    @Throws(DataKeyServiceUnavailableException::class)
    fun testBatchDataKey_WhenErrorsOccur_WillRetryUntilSuccessful() {
        val responseBody = """
            |{
            |    "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |    "plaintextDataKey": "PLAINTEXT_DATAKEY",
            |    "ciphertextDataKey": "CIPHERTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val statusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(statusLine.statusCode).willReturn(503, 503, 201)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpGet::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.batchDataKey()

        val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
        assertEquals(expectedResult, dataKeyResult)

        val argumentCaptor = ArgumentCaptor.forClass(HttpGet::class.java)
        verify(httpClient, times(3)).execute(argumentCaptor.capture())
        assertEquals("http://dummydks/datakey?correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_HappyCase_CallsServerOnce_AndReturnsUnencryptedData() {
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
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)
        val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_WhenErrorOccur_WillRetryUntilSuccessful() {
        val responseBody = """
            |{
            |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
            |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
            |}
        """.trimMargin()

        val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
        val mockStatusLine = mock(StatusLine::class.java)
        val entity = mock(HttpEntity::class.java)
        given(entity.content).willReturn(byteArrayInputStream)
        given(mockStatusLine.statusCode).willReturn(503, 503, 200)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(mockStatusLine)
        given(httpResponse.entity).willReturn(entity)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)
        val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
        verify(httpClient, times(3)).execute(argumentCaptor.capture())
        assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_HappyCase_WillCallServerOnce_AndCacheResponse() {
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
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
        assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)

        keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

        val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
        verify(httpClient, times(1)).execute(argumentCaptor.capture())
        assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
    }

    @Test
    fun testDecryptKey_WithABadKey_WillCallServerOnce_AndNotRetry() {
        val statusLine = mock(StatusLine::class.java)
        given(statusLine.statusCode).willReturn(400)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyDecryptionException")
        } catch (ex: DataKeyDecryptionException) {
            assertEquals("Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with keyEncryptionKeyId: '123', dks_correlation_id: '$dksCallId' data key service returned status code '400'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    fun testDecryptKey_ServerError_WillCauseRetryMaxTimes() {
        val statusLine = mock(StatusLine::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with keyEncryptionKeyId: '123', dks_correlation_id: '$dksCallId' data key service returned status code '503'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

    @Test
    fun testDecryptKey_UnknownHttpError_WillCauseRetryMaxTimes() {
        val statusLine = mock(StatusLine::class.java)
        given(statusLine.statusCode).willReturn(503)
        val httpResponse = mock(CloseableHttpResponse::class.java)
        given(httpResponse.statusLine).willReturn(statusLine)
        val httpClient = mock(CloseableHttpClient::class.java)
        given(httpClient.execute(any(HttpPost::class.java))).willThrow(RuntimeException("Boom!"))
        given(httpClientProvider.client()).willReturn(httpClient)
        val dksCallId = nextDksCorrelationId()
        whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)

        try {
            keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            fail("Should throw a DataKeyServiceUnavailableException")
        } catch (ex: DataKeyServiceUnavailableException) {
            assertEquals("Error contacting data key service: 'java.lang.RuntimeException: Boom!', dks_correlation_id: '$dksCallId'", ex.message)
            val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
            verify(httpClient, times(5)).execute(argumentCaptor.capture())
            assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
        }
    }

}
