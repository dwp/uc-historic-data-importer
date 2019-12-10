package app.batch

import app.configuration.S3Configuration
import app.domain.InputStreamPair
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.*
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.any
import org.mockito.BDDMockito.given
import org.mockito.Mockito
import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.retry.annotation.EnableRetry
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner

@EnableRetry
@RunWith(SpringRunner::class)
@SpringBootTest(classes = [S3Reader::class, S3Configuration::class, KeyPairGenerator::class, S3Helper::class])
@TestPropertySource(properties = [
    "aws.region=eu-west-1",
    "s3.bucket=bucket1",
    "s3.prefix.folder=test",
    "s3.suffixes.csv=output1",
    "s3.key.regex=([\\w]*\\.[\\w]*\\.[0-9]{4}\\.json\\.gz)",
    "s3.data.key.extension=\\.enc$",
    "s3.metadata.key.extension=\\.encryption\\.json$",
    "s3.suffixes.csv=output1"
])
class S3ReaderRetryTest {

    private val BUCKET_NAME = "bucket1"

    @MockBean
    private lateinit var s3Client: AmazonS3

    @Autowired
    private lateinit var s3Reader: ItemReader<InputStreamPair>

    @Before
    fun setUp() {
        Mockito.reset(s3Client)
    }

    @Test
    fun Test_s3getObject_WhenExceptionOccur_WillretryUntilSuccessful() {

        val page1Object1Key = "database1.collection1.0001.json.gz.enc"
        val page1Object2Key = "database1.collection1.0001.json.gz.encryption.json"

        val page1ObjectSummary1 = mockS3ObjectSummary(page1Object1Key)
        val page1ObjectSummary2 = mockS3ObjectSummary(page1Object2Key)

        val resultsPage1 = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn listOf(page1ObjectSummary1, page1ObjectSummary2)
        }
        given(s3Client.listObjectsV2(any(ListObjectsV2Request::class.java)))
            .willReturn(resultsPage1)

        val page1Object1 = mockS3Object()
        val page1Object2 = mockS3Object()

        given(s3Client.getObject(BUCKET_NAME, page1Object1Key)).willThrow(RuntimeException()).willReturn(page1Object1)
        given(s3Client.getObject(BUCKET_NAME, page1Object2Key)).willThrow(RuntimeException()).willReturn(page1Object2)

        s3Reader.read()

        verify(s3Client, times(2)).getObject(BUCKET_NAME, page1Object1Key)
        verify(s3Client, times(2)).getObject(BUCKET_NAME, page1Object2Key)

    }

    @Test
    fun Test_s3getObject_WhenExceptionOccur_Until_MaxLimit() {

        val page1Object1Key = "database1.collection1.0001.json.gz.enc"
        val page1Object2Key = "database1.collection1.0001.json.gz.encryption.json"

        val page1ObjectSummary1 = mockS3ObjectSummary(page1Object1Key)
        val page1ObjectSummary2 = mockS3ObjectSummary(page1Object2Key)

        val resultsPage1 = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn listOf(page1ObjectSummary1, page1ObjectSummary2)
        }
        given(s3Client.listObjectsV2(any(ListObjectsV2Request::class.java)))
            .willReturn(resultsPage1)

        given(s3Client.getObject(BUCKET_NAME, page1Object1Key)).willThrow(RuntimeException())
        given(s3Client.getObject(BUCKET_NAME, page1Object2Key)).willThrow(RuntimeException())

        try {
            s3Reader.read()
        } catch (ex: Exception) {
            verify(s3Client, times(5)).getObject(BUCKET_NAME, page1Object1Key)
        }
    }

    private fun mockS3ObjectSummary(objectKey: String) =
        mock<S3ObjectSummary> {
            on { key } doReturn objectKey
        }

    private fun mockS3Object() =
        mock<S3Object> {
            on { objectContent } doReturn mock<S3ObjectInputStream>()
        }

}
