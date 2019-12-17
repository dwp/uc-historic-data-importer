package app.batch

import app.domain.DecompressedStream
import ch.qos.logback.classic.spi.ILoggingEvent
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.PutObjectRequest
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verify
import org.junit.Test

import org.junit.Assert.*
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import java.io.*

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [LintWriter::class, MessageUtils::class])
@ActiveProfiles("lintWriter")
@TestPropertySource(properties = [
    "s3.errors.bucket=bucket",
    "s3.errors.prefix=prefix"
])
class LintWriterTest {

    @MockBean
    private lateinit var s3: AmazonS3

    @Autowired
    private lateinit var lintWriter: LintWriter

    @Test
    fun write() {
        val bytes = ByteArrayOutputStream()
        BufferedWriter(OutputStreamWriter(bytes)).use {writer ->
            for (i in 0..20) {
                writer.write("[1, 2, 3]\n")
                writer.write("}\n")
                writer.write("\"\"\n")
            }
        }
        val inputStream = ByteArrayInputStream(bytes.toByteArray())
        val decompressedStream = DecompressedStream(inputStream, "phoneyfilename.txt")
        val items = mutableListOf<DecompressedStream>()
        items.add(decompressedStream)
        lintWriter.write(items)
        val captor = argumentCaptor<PutObjectRequest>()
        verify(s3, times(1)).putObject(captor.capture())

        val putObjectRequest = captor.firstValue
        val input = putObjectRequest.inputStream

        var expected = ""

        for (i in 0..27 step 3) {
            expected += """
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,${i + 1}
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},${i + 2}
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,${i + 3}

""".trimIndent()
        }

        val actualContents = ByteArrayOutputStream()
        BufferedReader(InputStreamReader(input)).forEachLine {
            actualContents.write("$it\n".toByteArray())
        }

        val actual = actualContents.toByteArray()

        assertEquals(expected, String(actual))
        assertEquals("bucket", putObjectRequest.bucketName)
        assertEquals("prefix/phoneyfilename.txt", putObjectRequest.key)
    }
}
