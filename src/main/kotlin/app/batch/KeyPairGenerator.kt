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

        if (unMatchedFlattened.isNotEmpty()) {
            logger.warn("Found unmatched keys not matching regex", "unmatched_count", "${unMatchedFlattened.count()}",
                "file_format", fileFormat.pattern, "unmatched_keys", unMatchedFlattened.joinToString(", "))
        }

        val keyPairs = matched.map { pair ->
            logger.info("Found matched key pair", "pair_key", "${pair.key}", "pair_value", "${pair.value}")
            val neitherDataNorMetadataKey =
                pair.value.filterNot { ti -> (ti.contains(dataFileExtension) || ti.contains(metadataFileExtension)) }
            val dataKey = pair.value.find { ti -> ti.contains(dataFileExtension) }
            val metadataKey = pair.value.find { ti -> ti.contains(metadataFileExtension) }

            if (neitherDataNorMetadataKey.isNotEmpty()) {
                logger.warn("Found file(s) that matched format but neither data or metadata file extensions", "bad_files", neitherDataNorMetadataKey.joinToString(", "))
            }

            KeyPair(dataKey, metadataKey)
        }
        validateKeyPairs(keyPairs)
        return keyPairs.filter { it.dataKey !== null && it.metadataKey != null }
    }

    fun validateKeyPairs(keyPairs: List<KeyPair>) {
        keyPairs.forEach {
            if (it.dataKey != null && it.metadataKey == null) {
                val metadataFileNotFoundError = "Metadata file not found for data file"
                logger.error(metadataFileNotFoundError, "data_file", "${it.dataKey}")
                throw RuntimeException("metadataFileNotFoundError: data_file: ${it.dataKey}")
            }
            else if (it.metadataKey != null && it.dataKey == null) {
                logger.error("Data file not found for metadata file", "metadata_file", "${it.metadataKey}")
            }
        }
    }

    companion object {
        val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(KeyPairGenerator::class.toString())
    }
}

