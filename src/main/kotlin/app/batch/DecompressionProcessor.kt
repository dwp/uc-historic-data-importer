package app.batch

import app.domain.DecompressedStream
import app.domain.DecryptedStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import java.util.zip.GZIPInputStream

@Component
class DecompressionProcessor : ItemProcessor<DecryptedStream, DecompressedStream> {

    override fun process(item: DecryptedStream): DecompressedStream? {
        val inputStream = item.inputStream
        val fileName = item.fileName
        try {
            logger.info("Starting decompression of the file '$fileName'.")
            logger.info("inputStream: '$inputStream'.")
//            val compressorInputStream = CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP,
//                inputStream)

            val decompressedStream = GZIPInputStream(inputStream)
//            logger.info("Compressed size of the file $fileName : ${compressorInputStream.compressedCount}")
            logger.info("Using java.util.zip.")
            return DecompressedStream(decompressedStream, fileName)
        }
        catch (e: Exception) {
            logger.error("Processing '$fileName': ${e.message}", e)
//            e.printStackTrace(System.err)
            val decompressionExceptionMsg = "Exception occurred when decompressing the gzip decrypted input stream from the file $fileName : ${e.message}"
//            logger.error(decompressionExceptionMsg)
            throw RuntimeException("$decompressionExceptionMsg : $e")
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DecompressionProcessor::class.toString())
    }
}
