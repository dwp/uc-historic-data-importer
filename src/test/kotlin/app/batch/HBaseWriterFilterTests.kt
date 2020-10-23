package app.batch

import app.domain.DataKeyResult
import app.domain.DecompressedStream
import app.domain.EncryptionResult
import app.services.CipherService
import app.services.FilterService
import app.services.KeyService
import app.services.S3Service
import com.amazonaws.services.s3.AmazonS3
import com.beust.klaxon.JsonObject
import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.*
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.boot.test.mock.mockito.SpyBean
import org.springframework.test.context.TestPropertySource
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.test.util.ReflectionTestUtils
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.security.Key

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [HBaseWriter::class])
@TestPropertySource(properties = [
    "s3.bucket=bucket",
    "hbase.retry.max.attempts=5",
    "hbase.retry.initial.backoff=1",
    "hbase.retry.backoff.multiplier=1",
    "max.batch.size.bytes=100"
])
class HBaseWriterFilterTests {
    val validFileName = "adb.collection.0001.json.gz.enc"

    @MockBean
    private lateinit var keyService: KeyService

    @MockBean
    private lateinit var s3: AmazonS3

    @MockBean
    private lateinit var filterService: FilterService

    @MockBean
    private lateinit var cipherService: CipherService

    @MockBean
    private lateinit var hbaseClient: HbaseClient

    @MockBean
    private lateinit var messageProducer: MessageProducer

    @MockBean
    private lateinit var messageUtils: MessageUtils

    @MockBean
    private lateinit var s3Service: S3Service

    @SpyBean
    private lateinit var hBaseWriter: HBaseWriter

    @Test
    fun shouldNotAddToBatchIfFiltered() {
        val date = "\$date"
        val validJson = """{
                |    "_id": {
                |        "declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"
                |    },
                |    "_lastModifiedDateTime": {
                |           "$date": "2010-01-01T00:00:00.000Z" 
                |    }
                |}""".trimMargin()

        val json = Gson().fromJson(validJson, com.google.gson.JsonObject::class.java)
        val dumpLine = json.toString()
        val dataKeyResult = DataKeyResult("", "", "")
        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)
        val encryptionResult = EncryptionResult("", "")
        whenever(cipherService.encrypt(any(), any())).thenReturn(encryptionResult)
        val jsonObject = JsonObject()
        whenever(messageUtils.parseGson(any())).thenReturn(json)
        whenever(messageUtils.parseJson(any())).thenReturn(jsonObject)
        whenever(messageUtils.getId(any())).thenReturn(jsonObject)
        whenever(hBaseWriter).getVersion(any(), any(), any(), any()).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(message)
        val formattedKey = "0000-0000-00001"
        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())
        doNothing().whenever(hBaseWriter).ensureTable("adb:collection")
        doNothing().whenever(hBaseWriter).putBatch(any(), any())
        val data = listOf(dumpLine)
        val inputStreams = mutableListOf(inputStream(data, validFileName))
        ReflectionTestUtils.setField(hBaseWriter, "runMode", "import")
        whenever(filterService.filterStatus(any(), any(), any())).thenReturn(FilterService.FilterStatus.FilterExists)
        hBaseWriter.write(inputStreams)
        verify(hBaseWriter, times(0)).addToBatch(any(), any(), any(), any())
    }

    @Test
    fun shouldAddToBatchIfNotFiltered() {
        val date = "\$date"
        val validJson = """{
                |    "_id": {
                |        "declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"
                |    },
                |    "_lastModifiedDateTime": {
                |           "$date": "2010-01-01T00:00:00.000Z" 
                |    }
                |}""".trimMargin()

        val json = Gson().fromJson(validJson, com.google.gson.JsonObject::class.java)
        val dumpLine = json.toString()
        val dataKeyResult = DataKeyResult("", "", "")
        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)
        val encryptionResult = EncryptionResult("", "")
        whenever(cipherService.encrypt(any(), any())).thenReturn(encryptionResult)
        val jsonObject = JsonObject()
        whenever(messageUtils.parseGson(any())).thenReturn(json)
        whenever(messageUtils.parseJson(any())).thenReturn(jsonObject)
        whenever(messageUtils.getId(any())).thenReturn(jsonObject)
        whenever(hBaseWriter).getVersion(any(), any(), any(), any()).thenReturn(100)
        val message = "message"
        whenever(messageProducer.produceMessage(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())).thenReturn(message)
        val formattedKey = "0000-0000-00001"
        whenever(messageUtils.generateKeyFromRecordBody(any())).thenReturn(formattedKey.toByteArray())
        doNothing().whenever(hBaseWriter).ensureTable("adb:collection")
        doNothing().whenever(hBaseWriter).putBatch(any(), any())
        val data = listOf(dumpLine)
        val inputStreams = mutableListOf(inputStream(data, validFileName))
        ReflectionTestUtils.setField(hBaseWriter, "runMode", "import")
        whenever(filterService.filterStatus(any(), any(), any())).thenReturn(FilterService.FilterStatus.DoNotFilter)
        hBaseWriter.write(inputStreams)
        verify(hBaseWriter, times(1)).addToBatch(any(), any(), any(), any())
    }

    private fun inputStream(data1: List<String>, fileName: String): DecompressedStream {
        val outputStream = ByteArrayOutputStream()
        data1.forEach {
            val nl = it + "\n"
            outputStream.write(nl.toByteArray())
        }
        val inputStream = ByteArrayInputStream(outputStream.toByteArray())
        val key = mock<Key>()
        return DecompressedStream(inputStream, fileName, key, "")
    }

}
