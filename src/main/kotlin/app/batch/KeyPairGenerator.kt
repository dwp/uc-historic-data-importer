package app.batch

import app.domain.KeyPair
import app.utils.logging.logError
import app.utils.logging.logInfo
import app.utils.logging.logWarn
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class KeyPairGenerator {

    fun generateKeyPairs(keys: List<String>, fileFormat: Regex, dataFileExtension: Regex, metadataFileExtension: Regex): List<KeyPair> {
        val keysMap = keys.groupBy {
            fileFormat.find(it)?.value
        }

        val (unMatched, matched) = keysMap.map { it }.partition { it.key == null }
        val unMatchedFlattened = unMatched.flatMap { it.value }

        logWarn(logger, "${unMatchedFlattened.count()} key(s) that don't match the given file fileFormat $fileFormat found")
        if (unMatchedFlattened.isNotEmpty()) {
            logWarn(logger, "Unmatched keys : ${unMatchedFlattened.joinToString(", ")}")
        }

        val keyPairs = matched.map { pair ->
            logInfo(logger, "Matched key : ${pair.key} Value : ${pair.value} \n")
            val neitherDataNorMetadataKey =
                pair.value.filterNot { ti -> (ti.contains(dataFileExtension) || ti.contains(metadataFileExtension)) }
            val dataKey = pair.value.find { ti -> ti.contains(dataFileExtension) }
            val metadatakey = pair.value.find { ti -> ti.contains(metadataFileExtension) }

            if (neitherDataNorMetadataKey.isNotEmpty()) {
                logWarn(logger, "${neitherDataNorMetadataKey.joinToString(", ")} matched file format but not data or metadata file extensions")
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
                logError(logger, metadataFileNotFoundError)
                throw RuntimeException(metadataFileNotFoundError)
            }
            else if (it.metadataKey != null && it.dataKey == null) {
                logError(logger, "Data file not found for the metadata file ${it.metadataKey}")
            }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(KeyPairGenerator::class.toString())
    }
}

