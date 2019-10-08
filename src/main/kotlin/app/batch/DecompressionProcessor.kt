package app.batch

import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component
import java.io.InputStream

@Component
class DecompressionProcessor : ItemProcessor<DecryptedStream, InputStream> {

    override fun process(item: DecryptedStream): InputStream? {
        val inputStream = item.inputStream
        val fileName = item.fileName
        try {
            val compressorInputStream = CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP,
                inputStream) as GzipCompressorInputStream
            logger.info("Compressed size of the file $fileName : ${compressorInputStream.compressedCount}")
            return compressorInputStream
        }
        catch (e: Exception) {
            val deCompressionExceptionMsg = "Exception occurred when decompressing the gzip decrypted input stream from the file $fileName"
            logger.error(deCompressionExceptionMsg)
            throw RuntimeException("$deCompressionExceptionMsg : $e")
        }
        finally {
            inputStream.close()
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(DecompressionProcessor::class.toString())
    }
}
