package app.services.impl

import app.services.S3Service
import com.amazonaws.SdkClientException
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.nhaarman.mockitokotlin2.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.junit4.SpringRunner

@RunWith(SpringRunner::class)
@EnableRetry
@SpringBootTest(classes = [S3ServiceImpl::class])
class S3ServiceImplTest {

    @SpyBean
    @Autowired
    private lateinit var s3Service: S3Service

    @MockBean
    private lateinit var amazonS3: AmazonS3

    @Before
    fun before() {
        reset(amazonS3)
    }

    @Test
    fun testObjectInputStreamRetries() {

        val inputStream = mock<S3ObjectInputStream>()

        val s3Object = mock<S3Object> {
            on { objectContent } doReturn inputStream
        }

        given(amazonS3.getObject(any<String>(), any<String>()))
                .willThrow(SdkClientException("first error"))
                .willThrow(SdkClientException("second error"))
                .willReturn(s3Object)

        s3Service.objectInputStream("bucket", "key")

        verify(s3Service, times(3)).objectInputStream("bucket", "key")
    }
}
