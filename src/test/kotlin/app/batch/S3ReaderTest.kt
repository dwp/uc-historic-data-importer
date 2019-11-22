package app.batch

import app.configuration.S3Configuration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.*
import com.nhaarman.mockitokotlin2.doReturn
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.apache.http.client.methods.HttpGet
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers
import org.mockito.BDDMockito.given
import org.mockito.Mockito
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.BufferedReader
import java.io.ByteArrayInputStream
import java.io.InputStream
import java.io.InputStreamReader
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

@RunWith(SpringRunner::class)
@ActiveProfiles("awsS3")
@SpringBootTest(classes = [S3Reader::class, S3Configuration::class, KeyPairGenerator::class])
@TestPropertySource(properties = [
    "aws.region=eu-west-1",
    "s3.bucket=bucket1",
    "s3.prefix.folder=test/output/",
    "s3.key.regex=([\\w]*\\.[\\w]*\\.[0-9]{4}\\.json\\.gz)",
    "s3.data.key.extension=\\.enc$",
    "s3.metadata.key.extension=\\.encryption\\.json$"
])
class S3ReaderTest {

    private val BUCKET_NAME1 = "bucket1"
    private val S3_PREFIX_FOLDER = "test/output/"
    private val VALID_DATA_KEY = "test/output/adb.collection.00001.json.gz.enc"
    private val VALID_METADATA_KEY = "test/output/adb.collection.00001.json.gz.encryption.json"
    private val OBJECT_CONTENT1 = "SAMPLE1"
    private val OBJECT_CONTENT2 = "SAMPLE2"

    private lateinit var listObjectsV2Result: ListObjectsV2Result
    private lateinit var s3ObjectSummary1: S3ObjectSummary
    private lateinit var s3ObjectSummary2: S3ObjectSummary
    private lateinit var s3Object1: S3Object
    private lateinit var s3Object2: S3Object

    @MockBean
    private lateinit var s3Client: AmazonS3

    @Autowired
    private lateinit var s3Reader: S3Reader

    @Before
    fun setUp() {
        s3Reader.reset()
        Mockito.reset(s3Client)
    }

    @Test
    fun should_page_when_results_truncated() {

        val bucket = "bucket1"
        val page1Object1Key = "database1.collection1.0001.json.gz.enc"
        val page1Object2Key = "database1.collection1.0001.json.gz.encryption.json"
        val page2Object1Key = "database1.collection2.0001.json.gz.enc"
        val page2Object2Key = "database1.collection2.0001.json.gz.encryption.json"
        val continuationToken = "CONTINUATION_TOKEN"

        val page1ObjectSummary1 = mockS3ObjectSummary(page1Object1Key)
        val page1ObjectSummary2 = mockS3ObjectSummary(page1Object2Key)

        val resultsPage1 = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn listOf(page1ObjectSummary1, page1ObjectSummary2)
            on { isTruncated } doReturn true
            on { nextContinuationToken } doReturn continuationToken
        }

        val page2ObjectSummary1 = mockS3ObjectSummary(page2Object1Key)
        val page2ObjectSummary2 = mockS3ObjectSummary(page2Object2Key)

        val resultsPage2 = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn listOf(page2ObjectSummary1, page2ObjectSummary2)
            on { isTruncated } doReturn false
        }

        given(s3Client.listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request::class.java)))
                .willReturn(resultsPage1)
                .willReturn(resultsPage2)

        val page1Object1 = mockS3Object()
        val page1Object2 = mockS3Object()
        val page2Object1 = mockS3Object()
        val page2Object2 = mockS3Object()

        given(s3Client.getObject(bucket, page1Object1Key)).willReturn(page1Object1)
        given(s3Client.getObject(bucket, page1Object2Key)).willReturn(page1Object2)
        given(s3Client.getObject(bucket, page2Object1Key)).willReturn(page2Object1)
        given(s3Client.getObject(bucket, page2Object2Key)).willReturn(page2Object2)

        s3Reader.read()

        verify(s3Client, times(2))
                .listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request::class.java))

    }

    @Test
    fun should_not_page_when_results_not_truncated() {

        val bucket = "bucket1"
        val page1Object1Key = "database1.collection1.0001.json.gz.enc"
        val page1Object2Key = "database1.collection1.0001.json.gz.encryption.json"

        val page1ObjectSummary1 = mockS3ObjectSummary(page1Object1Key)
        val page1ObjectSummary2 = mockS3ObjectSummary(page1Object2Key)

        val resultsPage1 = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn listOf(page1ObjectSummary1, page1ObjectSummary2)
            on { isTruncated } doReturn false
        }
        given(s3Client.listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request::class.java)))
                .willReturn(resultsPage1)
        val page1Object1 = mockS3Object()
        val page1Object2 = mockS3Object()

        given(s3Client.getObject(bucket, page1Object1Key)).willReturn(page1Object1)
        given(s3Client.getObject(bucket, page1Object2Key)).willReturn(page1Object2)

        s3Reader.read()

        verify(s3Client, times(1))
                .listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request::class.java))

    }

    private fun mockS3Object() =
            mock<S3Object> {
                on { objectContent } doReturn mock<S3ObjectInputStream>()
            }

    private fun mockS3ObjectSummary(objectKey: String)=
            mock<S3ObjectSummary> {
                on { key } doReturn objectKey
            }


    @Test
    fun should_Read_Data_And_Metadata_Files_In_A_Given_Prefix() {
        listObjectsV2Result = ListObjectsV2Result()
        listObjectsV2Result.prefix = S3_PREFIX_FOLDER

        s3ObjectSummary1 = S3ObjectSummary()
        s3ObjectSummary1.bucketName = BUCKET_NAME1
        s3ObjectSummary1.key = VALID_DATA_KEY

        s3ObjectSummary2 = S3ObjectSummary()
        s3ObjectSummary2.bucketName = BUCKET_NAME1
        s3ObjectSummary2.key = VALID_METADATA_KEY

        listObjectsV2Result.objectSummaries.add(s3ObjectSummary1)
        listObjectsV2Result.objectSummaries.add(s3ObjectSummary2)

        s3Object1 = S3Object()
        s3Object1.bucketName = BUCKET_NAME1
        s3Object1.key = VALID_DATA_KEY
        s3Object1.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT1.toByteArray()), HttpGet())

        s3Object2 = S3Object()
        s3Object2.bucketName = BUCKET_NAME1
        s3Object2.key = VALID_METADATA_KEY
        s3Object2.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT2.toByteArray()), HttpGet())

        given(s3Client.listObjectsV2(ArgumentMatchers.any(ListObjectsV2Request::class.java))).willReturn(listObjectsV2Result)
        given(s3Client.getObject(BUCKET_NAME1, VALID_DATA_KEY)).willReturn(s3Object1)
        given(s3Client.getObject(BUCKET_NAME1, VALID_METADATA_KEY)).willReturn(s3Object2)

        val encryptedStream1 = s3Reader.read()
        val dataStream = encryptedStream1?.dataInputStream
        val metadataStream = encryptedStream1?.metadataInputStream

        assertObjectContent(OBJECT_CONTENT1, dataStream)
        assertObjectContent(OBJECT_CONTENT2, metadataStream)
    }

    @Test(expected = RuntimeException::class)
    fun should_Throw_Exception_When_Metadata_File_Missing_In_A_Given_Prefix() {

        listObjectsV2Result = ListObjectsV2Result()
        listObjectsV2Result.prefix = S3_PREFIX_FOLDER

        s3ObjectSummary1 = S3ObjectSummary()
        s3ObjectSummary1.bucketName = BUCKET_NAME1
        s3ObjectSummary1.key = VALID_DATA_KEY


        listObjectsV2Result.objectSummaries.add(s3ObjectSummary1)

        s3Object1 = S3Object()
        s3Object1.bucketName = BUCKET_NAME1
        s3Object1.key = VALID_DATA_KEY
        s3Object1.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT1.toByteArray()), HttpGet())


        given(s3Client.listObjectsV2(BUCKET_NAME1, S3_PREFIX_FOLDER)).willReturn(listObjectsV2Result)
        given(s3Client.getObject(BUCKET_NAME1, VALID_DATA_KEY)).willReturn(s3Object1)

        given(s3Client.listObjectsV2(BUCKET_NAME1, S3_PREFIX_FOLDER)).willReturn(listObjectsV2Result)
        given(s3Client.getObject(BUCKET_NAME1, VALID_DATA_KEY)).willReturn(s3Object1)

        s3Reader.read()

    }

    private fun assertObjectContent(objectContent: String, actualStream: InputStream?) {
        val textBuilder = StringBuilder()
        BufferedReader(InputStreamReader(actualStream, Charset.forName(StandardCharsets.UTF_8.name()))).use { reader ->
            var c = 0
            while (c != -1) {
                c = reader.read()
                if (c != -1) {
                    textBuilder.append(c.toChar())
                }
            }
        }
        assertTrue(objectContent.equals(textBuilder.toString().trim()))
    }
}
