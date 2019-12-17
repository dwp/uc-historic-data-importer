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

    //data class DecompressedStream(val inputStream: InputStream, val fileName: String)
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


        val expected = """
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,1
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},2
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,3
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,4
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},5
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,6
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,7
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},8
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,9
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,10
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},11
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,12
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,13
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},14
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,15
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,16
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},17
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,18
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,19
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},20
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,21
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,22
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},23
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,24
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,25
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},26
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,27
        "[1, 2, 3]",Expected a com.google.gson.JsonObject but was com.google.gson.JsonArray,28
        },com.google.gson.stream.MalformedJsonException: Expected value at line 1 column 1 path ${'$'},29
        ""${'"'}${'"'}${'"'}${'"'},Expected a com.google.gson.JsonObject but was com.google.gson.JsonPrimitive,30

        """.trimIndent()

        val actualContents = ByteArrayOutputStream()
        BufferedReader(InputStreamReader(input)).forEachLine {
            actualContents.write("$it\n".toByteArray())
        }

        val actual = actualContents.toByteArray()

        assertEquals(expected, String(actual))
    }
}
