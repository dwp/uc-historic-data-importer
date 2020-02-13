package app.batch

import app.domain.DecompressedStream
import app.domain.DecryptedStream
import app.utils.logging.JsonLoggerWrapper
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class DecompressionProcessor : ItemProcessor<DecryptedStream, DecompressedStream> {

    override fun process(item: DecryptedStream): DecompressedStream? {
        val inputStream = item.inputStream
        val fileName = item.fileName
        try {
            logger.info("Starting decompression of file", "file_name", fileName, "inputStream", "$inputStream")
            val compressorInputStream = CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP,
                inputStream) as GzipCompressorInputStream
            logger.info("Compressed file", "file_name", fileName, "file_size", "${compressorInputStream.compressedCount}")
            return DecompressedStream(compressorInputStream, fileName, item.key, item.iv)
        }
        catch (e: Exception) {
            val decompressionExceptionMsg = "Exception occurred when decompressing the gzip decrypted input stream from the file"
            logger.error(decompressionExceptionMsg, e, "file_name", fileName, "error_message", "${e.message}")
            throw RuntimeException("$decompressionExceptionMsg, file_name: $fileName, exception: $e")
        }
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(DecompressionProcessor::class.toString())
    }
}
