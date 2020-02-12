package app.batch

import app.domain.KeyPair
import app.utils.logging.JsonLoggerWrapper
import org.springframework.stereotype.Component

@Component
class KeyPairGenerator {

    fun generateKeyPairs(keys: List<String>, fileFormat: Regex, dataFileExtension: Regex, metadataFileExtension: Regex): List<KeyPair> {
        val keysMap = keys.groupBy {
            fileFormat.find(it)?.value
        }

        val (unMatched, matched) = keysMap.map { it }.partition { it.key == null }
        val unMatchedFlattened = unMatched.flatMap { it.value }

        logger.warn("${unMatchedFlattened.count()} key(s) that don't match the given file fileFormat $fileFormat found")
        if (unMatchedFlattened.isNotEmpty()) {
            logger.warn("Unmatched keys : ${unMatchedFlattened.joinToString(", ")}")
        }

        val keyPairs = matched.map { pair ->
            logger.info("Matched key : ${pair.key} Value : ${pair.value} \n")
            val neitherDataNorMetadataKey =
                pair.value.filterNot { ti -> (ti.contains(dataFileExtension) || ti.contains(metadataFileExtension)) }
            val dataKey = pair.value.find { ti -> ti.contains(dataFileExtension) }
            val metadatakey = pair.value.find { ti -> ti.contains(metadataFileExtension) }

            if (neitherDataNorMetadataKey.isNotEmpty()) {
                logger.warn("${neitherDataNorMetadataKey.joinToString(", ")} matched file format but not data or metadata file extensions")
            }

            KeyPair(dataKey, metadatakey)
        }
        validateKeyPairs(keyPairs)
        return keyPairs.filter { it.dataKey !== null && it.metadataKey != null }
    }

    fun validateKeyPairs(keyPairs: List<KeyPair>) {
        keyPairs.forEach {
            if (it.dataKey != null && it.metadataKey == null) {
                val metadataFileNotFoundError = "Metadata file not found for the data file ${it.dataKey}"
                logger.error(metadataFileNotFoundError)
                throw RuntimeException(metadataFileNotFoundError)
            }
            else if (it.metadataKey != null && it.dataKey == null) {
                logger.error("Data file not found for the metadata file ${it.metadataKey}")
            }
        }
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(KeyPairGenerator::class.toString())
    }
}

