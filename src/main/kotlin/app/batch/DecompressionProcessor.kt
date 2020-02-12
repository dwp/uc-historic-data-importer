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
            logger.info("Starting decompression of the file $fileName")
            logger.info("inputStream: '$inputStream'")
            val compressorInputStream = CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP,
                inputStream) as GzipCompressorInputStream
            logger.info("Compressed size of the file $fileName : ${compressorInputStream.compressedCount}")
            return DecompressedStream(compressorInputStream, fileName, item.key, item.iv)
        }
        catch (e: Exception) {

            val decompressionExceptionMsg = "Exception occurred when decompressing the gzip decrypted input stream from the file $fileName : ${e.message}"
            logger.error(decompressionExceptionMsg, e)
            throw RuntimeException("$decompressionExceptionMsg : $e")
        }
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(DecompressionProcessor::class.toString())
    }
}
