package app.batch

import app.configuration.S3Configuration
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.*
import com.nhaarman.mockitokotlin2.*
import org.apache.http.client.methods.HttpGet
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.ArgumentMatchers.any
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
import java.lang.RuntimeException
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets

@RunWith(SpringRunner::class)
@ActiveProfiles("awsS3")
@SpringBootTest(classes = [S3Reader::class, S3Configuration::class, KeyPairGenerator::class])
@TestPropertySource(properties = [
    "aws.region=eu-west-1",
    "s3.bucket=bucket1",
    "s3.prefix.folder=test",
    "s3.suffixes.csv=output1",
    "s3.key.regex=([\\w]*\\.[\\w]*\\.[0-9]{4}\\.json\\.gz)",
    "s3.data.key.extension=\\.enc$",
    "s3.metadata.key.extension=\\.encryption\\.json$"
])
class S3ReaderTest {

    private val BUCKET_NAME = "bucket1"
    private val S3_PREFIX_FOLDER = "test"
    private val S3_SUFFIXES_CSV = "output1,output2"
    private val S3_SUFFIX_1 = "output1"
    private val S3_SUFFIX_2 = "output2"
    private val VALID_DATA_KEY_1 = "test/output1/adb.collection.00001.json.gz.enc"
    private val VALID_METADATA_KEY_1 = "test/output1/adb.collection.00001.json.gz.encryption.json"
    private val VALID_DATA_KEY_2 = "test/output2/adb.other-collection.00002.json.gz.enc"
    private val VALID_METADATA_KEY_2 = "test/output2/adb.other-collection.00002.json.gz.encryption.json"
    private val OBJECT_CONTENT_1_1 = "SAMPLE_1_1"
    private val OBJECT_CONTENT_1_2 = "SAMPLE_1_2"
    private val OBJECT_CONTENT_2_1 = "SAMPLE_2_1"
    private val OBJECT_CONTENT_2_2 = "SAMPLE_2_2"

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

        given(s3Client.listObjectsV2(any(ListObjectsV2Request::class.java)))
                .willReturn(resultsPage1)
                .willReturn(resultsPage2)

        val page1Object1 = mockS3Object()
        val page1Object2 = mockS3Object()
        val page2Object1 = mockS3Object()
        val page2Object2 = mockS3Object()

        given(s3Client.getObject(BUCKET_NAME, page1Object1Key)).willReturn(page1Object1)
        given(s3Client.getObject(BUCKET_NAME, page1Object2Key)).willReturn(page1Object2)
        given(s3Client.getObject(BUCKET_NAME, page2Object1Key)).willReturn(page2Object1)
        given(s3Client.getObject(BUCKET_NAME, page2Object2Key)).willReturn(page2Object2)

        s3Reader.s3SuffixesCsv = S3_SUFFIX_1
        s3Reader.read()
        s3Reader.read()

        verify(s3Client, times(2)).listObjectsV2(any(ListObjectsV2Request::class.java))
        verify(s3Client, times(1)).getObject(BUCKET_NAME, page1Object1Key)
        verify(s3Client, times(1)).getObject(BUCKET_NAME, page1Object2Key)
        verify(s3Client, times(1)).getObject(BUCKET_NAME, page2Object1Key)
        verify(s3Client, times(1)).getObject(BUCKET_NAME, page2Object2Key)
        verify(resultsPage1, times(1)).objectSummaries
        verify(resultsPage1, times(1)).isTruncated
        verify(resultsPage1, times(1)).nextContinuationToken
        verify(resultsPage2, times(1)).objectSummaries
        verify(resultsPage2, times(1)).isTruncated
        verify(resultsPage2, times(1)).nextContinuationToken

        verifyNoMoreInteractions(s3Client, resultsPage1, resultsPage2)
    }

    @Test
    fun should_not_page_when_results_not_truncated() {
        val page1Object1Key = "database1.collection1.0001.json.gz.enc"
        val page1Object2Key = "database1.collection1.0001.json.gz.encryption.json"

        val page1ObjectSummary1 = mockS3ObjectSummary(page1Object1Key)
        val page1ObjectSummary2 = mockS3ObjectSummary(page1Object2Key)

        val resultsPage1 = mock<ListObjectsV2Result> {
            on { objectSummaries } doReturn listOf(page1ObjectSummary1, page1ObjectSummary2)
            on { isTruncated } doReturn false
        }
        given(s3Client.listObjectsV2(any(ListObjectsV2Request::class.java)))
                .willReturn(resultsPage1)
        val page1Object1 = mockS3Object()
        val page1Object2 = mockS3Object()

        given(s3Client.getObject(BUCKET_NAME, page1Object1Key)).willReturn(page1Object1)
        given(s3Client.getObject(BUCKET_NAME, page1Object2Key)).willReturn(page1Object2)

        s3Reader.s3SuffixesCsv = S3_SUFFIX_1
        s3Reader.read()

        verify(s3Client, times(1)).listObjectsV2(any(ListObjectsV2Request::class.java))
        verify(s3Client, times(1)).getObject(BUCKET_NAME, page1Object1Key)
        verify(s3Client, times(1)).getObject(BUCKET_NAME, page1Object2Key)
        verify(resultsPage1, times(1)).objectSummaries
        verify(resultsPage1, times(1)).isTruncated
        verify(resultsPage1, times(1)).nextContinuationToken
        verifyNoMoreInteractions(s3Client, resultsPage1)
    }

    @Test
    fun should_use_all_csv_prefixes() {

        val listObjectsV2Result_1 = ListObjectsV2Result()
        listObjectsV2Result_1.prefix = "$S3_PREFIX_FOLDER/$S3_SUFFIX_1"

        val s3ObjectSummary_1a = S3ObjectSummary()
        s3ObjectSummary_1a.bucketName = BUCKET_NAME
        s3ObjectSummary_1a.key = VALID_DATA_KEY_1

        val s3ObjectSummary_1b = S3ObjectSummary()
        s3ObjectSummary_1b.bucketName = BUCKET_NAME
        s3ObjectSummary_1b.key = VALID_METADATA_KEY_1

        listObjectsV2Result_1.objectSummaries.add(s3ObjectSummary_1a)
        listObjectsV2Result_1.objectSummaries.add(s3ObjectSummary_1b)

        val s3Object_1a = S3Object()
        s3Object_1a.bucketName = BUCKET_NAME
        s3Object_1a.key = VALID_DATA_KEY_1
        s3Object_1a.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT_1_1.toByteArray()), HttpGet())

        val s3Object_1b = S3Object()
        s3Object_1b.bucketName = BUCKET_NAME
        s3Object_1b.key = VALID_METADATA_KEY_1
        s3Object_1b.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT_1_2.toByteArray()), HttpGet())

        val listObjectsV2Result_2 = ListObjectsV2Result()
        listObjectsV2Result_2.prefix = "$S3_PREFIX_FOLDER/$S3_SUFFIX_2"

        val s3ObjectSummary_2a = S3ObjectSummary()
        s3ObjectSummary_2a.bucketName = BUCKET_NAME
        s3ObjectSummary_2a.key = VALID_DATA_KEY_2

        val s3ObjectSummary_2b = S3ObjectSummary()
        s3ObjectSummary_2b.bucketName = BUCKET_NAME
        s3ObjectSummary_2b.key = VALID_METADATA_KEY_2

        listObjectsV2Result_1.objectSummaries.add(s3ObjectSummary_2a)
        listObjectsV2Result_1.objectSummaries.add(s3ObjectSummary_2b)

        val s3Object_2a = S3Object()
        s3Object_2a.bucketName = BUCKET_NAME
        s3Object_2a.key = VALID_DATA_KEY_2
        s3Object_2a.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT_2_1.toByteArray()), HttpGet())

        val s3Object_2b = S3Object()
        s3Object_2b.bucketName = BUCKET_NAME
        s3Object_2b.key = VALID_METADATA_KEY_2
        s3Object_2b.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT_2_2.toByteArray()), HttpGet())

        given(s3Client.listObjectsV2(any(ListObjectsV2Request::class.java)))
                .willReturn(
                        listObjectsV2Result_1,
                        listObjectsV2Result_2)
        given(s3Client.getObject(BUCKET_NAME, VALID_DATA_KEY_1)).willReturn(s3Object_1a)
        given(s3Client.getObject(BUCKET_NAME, VALID_METADATA_KEY_1)).willReturn(s3Object_1b)
        given(s3Client.getObject(BUCKET_NAME, VALID_DATA_KEY_2)).willReturn(s3Object_2a)
        given(s3Client.getObject(BUCKET_NAME, VALID_METADATA_KEY_2)).willReturn(s3Object_2b)

        s3Reader.s3SuffixesCsv = S3_SUFFIXES_CSV

        val encryptedStream1 = s3Reader.read()
        val dataStream1 = encryptedStream1?.dataInputStream
        val metadataStream1 = encryptedStream1?.metadataInputStream
        assertObjectContent(OBJECT_CONTENT_1_1, dataStream1)
        assertObjectContent(OBJECT_CONTENT_1_2, metadataStream1)

        val encryptedStream2 = s3Reader.read()
        val dataStream2 = encryptedStream2?.dataInputStream
        val metadataStream2 = encryptedStream2?.metadataInputStream
        assertObjectContent(OBJECT_CONTENT_2_1, dataStream2)
        assertObjectContent(OBJECT_CONTENT_2_2, metadataStream2)

        verify(s3Client, times(2)).listObjectsV2(any(ListObjectsV2Request::class.java))
        verify(s3Client, times(1)).getObject(BUCKET_NAME, VALID_DATA_KEY_1)
        verify(s3Client, times(1)).getObject(BUCKET_NAME, VALID_METADATA_KEY_1)
        verify(s3Client, times(1)).getObject(BUCKET_NAME, VALID_DATA_KEY_2)
        verify(s3Client, times(1)).getObject(BUCKET_NAME, VALID_METADATA_KEY_2)
        verifyNoMoreInteractions(s3Client)
    }

    @Test
    fun should_read_data_and_metadata_files_in_a_given_prefix() {

        val listObjectsV2Result_1 = ListObjectsV2Result()
        listObjectsV2Result_1.prefix = "$S3_PREFIX_FOLDER/$S3_SUFFIX_1"

        val s3ObjectSummary_1a = S3ObjectSummary()
        s3ObjectSummary_1a.bucketName = BUCKET_NAME
        s3ObjectSummary_1a.key = VALID_DATA_KEY_1

        val s3ObjectSummary_1b = S3ObjectSummary()
        s3ObjectSummary_1b.bucketName = BUCKET_NAME
        s3ObjectSummary_1b.key = VALID_METADATA_KEY_1

        listObjectsV2Result_1.objectSummaries.add(s3ObjectSummary_1a)
        listObjectsV2Result_1.objectSummaries.add(s3ObjectSummary_1b)

        val s3Object_1a = S3Object()
        s3Object_1a.bucketName = BUCKET_NAME
        s3Object_1a.key = VALID_DATA_KEY_1
        s3Object_1a.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT_1_1.toByteArray()), HttpGet())

        val s3Object_1b = S3Object()
        s3Object_1b.bucketName = BUCKET_NAME
        s3Object_1b.key = VALID_METADATA_KEY_1
        s3Object_1b.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT_1_2.toByteArray()), HttpGet())

        given(s3Client.listObjectsV2(any(ListObjectsV2Request::class.java))).willReturn(listObjectsV2Result_1)
        given(s3Client.getObject(BUCKET_NAME, VALID_DATA_KEY_1)).willReturn(s3Object_1a)
        given(s3Client.getObject(BUCKET_NAME, VALID_METADATA_KEY_1)).willReturn(s3Object_1b)

        s3Reader.s3SuffixesCsv = S3_SUFFIX_1
        val encryptedStream1 = s3Reader.read()
        val dataStream = encryptedStream1?.dataInputStream
        val metadataStream = encryptedStream1?.metadataInputStream

        assertObjectContent(OBJECT_CONTENT_1_1, dataStream)
        assertObjectContent(OBJECT_CONTENT_1_2, metadataStream)

        verify(s3Client, times(1)).listObjectsV2(any(ListObjectsV2Request::class.java))
        verify(s3Client, times(1)).getObject(BUCKET_NAME, VALID_DATA_KEY_1)
        verify(s3Client, times(1)).getObject(BUCKET_NAME, VALID_METADATA_KEY_1)
        verifyNoMoreInteractions(s3Client)
    }

    @Test
    fun should_throw_exception_when_metadata_file_missing_in_a_given_prefix() {

        val listObjectsV2Result_1 = ListObjectsV2Result()
        listObjectsV2Result_1.prefix = "$S3_PREFIX_FOLDER/$S3_SUFFIX_1"

        val s3ObjectSummary_1a = S3ObjectSummary()
        s3ObjectSummary_1a.bucketName = BUCKET_NAME
        s3ObjectSummary_1a.key = VALID_DATA_KEY_1

        listObjectsV2Result_1.objectSummaries.add(s3ObjectSummary_1a)

        val s3Object_1a = S3Object()
        s3Object_1a.bucketName = BUCKET_NAME
        s3Object_1a.key = VALID_DATA_KEY_1
        s3Object_1a.objectContent = S3ObjectInputStream(ByteArrayInputStream(OBJECT_CONTENT_1_1.toByteArray()), HttpGet())

        given(s3Client.listObjectsV2(BUCKET_NAME, S3_PREFIX_FOLDER)).willReturn(listObjectsV2Result_1)
        given(s3Client.getObject(BUCKET_NAME, VALID_DATA_KEY_1)).willReturn(s3Object_1a)

        s3Reader.s3SuffixesCsv = S3_SUFFIX_1
        try {
            s3Reader.read()
            fail("Expected an exception")
        } catch (expected: RuntimeException) {
            assertEquals("java.lang.IllegalStateException: results must not be null", expected.toString())

            verify(s3Client, times(1)).listObjectsV2(any(ListObjectsV2Request::class.java))
            verifyNoMoreInteractions(s3Client)
        }
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

    private fun mockS3Object() =
            mock<S3Object> {
                on { objectContent } doReturn mock<S3ObjectInputStream>()
            }

    private fun mockS3ObjectSummary(objectKey: String) =
            mock<S3ObjectSummary> {
                on { key } doReturn objectKey
            }
}
