package app.batch

import app.domain.DecompressedStream
import app.services.CipherService
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.PutObjectRequest
import org.apache.commons.lang.StringEscapeUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Profile
import org.springframework.stereotype.Component
import java.io.*

@Component
@Profile("lintWriter")
class LintWriter(private val s3: AmazonS3, private val messageUtils: MessageUtils) : ItemWriter<DecompressedStream> {

    @Value("\${max.stream.attempts:10}")
    private lateinit var maxStreamAttempts: String

    @Value("\${s3.invalid.data.max.amount.per.error:10}")
    private lateinit var maxErrorsToLog: String

    @Value("\${s3.invalid.data.bucket}")
    private lateinit var bucket: String

    @Value("\${s3.invalid.data.prefix}")
    private lateinit var prefix: String

    @Value("\${s3.bucket}")
    private lateinit var s3bucket: String

    @Autowired
    private lateinit var cipherService: CipherService

    override fun write(items: MutableList<out DecompressedStream>) {
        items.forEach { input ->
            logger.info("Processing '${input.fileName}'.")
            val fileName = input.fileName
            var lineNo = 0
            var succeeded = false
            var attempts = 0
            var inputStream = input.inputStream
            while (!succeeded && attempts < maxStreamAttempts.toInt()) {
                try {
                    ++attempts
                    val errors = mutableListOf<ErrorRecord>()
                    val counts = mutableMapOf<String, Int>()
                    getBufferedReader(inputStream).forEachLine { line ->
                        ++lineNo
                        try {
                            messageUtils.parseGson(line)
                        } catch (e: Exception) {
                            val key = e.message ?: ""
                            logger.error("Error processing record $lineNo from '$fileName': '${e.message}'.")
                            val count = counts.getOrDefault(key, 0)
                            if (count < maxErrorsToLog.toInt()) {
                                errors.add(ErrorRecord(line, key, lineNo))
                                counts.put(key, count + 1)
                            }
                        }
                    }

                    val output = ByteArrayOutputStream()
                    BufferedWriter(OutputStreamWriter(output)).use { writer ->
                        errors.forEach {
                            writer.write("$it\n")
                        }
                    }

                    val byteArray = output.toByteArray()

                    val metadata = ObjectMetadata().apply {
                        contentType = "text/csv"
                        addUserMetadata("x-amz-meta-title", fileName)
                        contentLength = byteArray.size.toLong()
                    }
                    val request = PutObjectRequest(bucket, "$prefix/$fileName", ByteArrayInputStream(byteArray), metadata)
                    s3.putObject(request)
                    succeeded = true
                } catch (e: Exception) {
                    try {
                        logger.warn("Error on attempt $attempts streaming '$fileName': '${e.message}'.")
                        inputStream.close()
                    } catch (e: Exception) {
                        logger.warn("Failed to close stream: '${e.message}'.")
                    }

                    inputStream = cipherService.decompressingDecryptingStream(s3.getObject(s3bucket, fileName).objectContent, input.key, input.iv)
                    lineNo = 0
                }
            }

            logger.info("Processed $lineNo records from the file $fileName")
        }
    }

    fun getBufferedReader(inputStream: InputStream): BufferedReader {
        return BufferedReader(InputStreamReader(inputStream))
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(LintWriter::class.toString())
    }
}

data class ErrorRecord(val text: String, val errorMessage: String, val lineNo: Int) {
    override fun toString(): String {
        return "${StringEscapeUtils.escapeCsv(text)},${StringEscapeUtils.escapeCsv(errorMessage)},$lineNo"
    }
}
