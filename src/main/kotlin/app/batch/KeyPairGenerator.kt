package app.batch

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class KeyPairGenerator {

    fun generateKeyPairs(keys: List<String>, fileFormat: Regex, dataFileExtension: Regex, metadataFileExtension: Regex): List<KeyPair> {
        val keysMap = keys.groupBy { fileFormat.find(it)?.value }
        val (unMatched, matched) = keysMap.map { it }.partition { it.key == null }
        val unMatchedFlattened = unMatched.flatMap { it.value }
        logger.warn("${unMatchedFlattened.count()} key(s) that don't match the given file fileFormat $fileFormat found")
        logger.info("Unmatched keys : ${unMatchedFlattened.joinToString(",")}")
        val keyPairs = matched.map {
            logger.info("Matched key : ${it.key} Value : ${it.value} \n")
            val neitherDataNorMetadataKey = it.value.filterNot { ti -> (ti.contains(dataFileExtension) || ti.contains(metadataFileExtension)) }
            logger.warn("${neitherDataNorMetadataKey.joinToString(",")} matched file format but not matched neither data nor metadata file extension")
            val dataKey = it.value.find { ti -> ti.contains(dataFileExtension) }
            val metadatakey = it.value.find { ti -> ti.contains(metadataFileExtension) }
            KeyPair(dataKey, metadatakey)
        }
        validateKeyPairs(keyPairs)
        return keyPairs.filter { it.dataKey !== null && it.metadataKey != null }
    }

    fun validateKeyPairs(keyPairs: List<KeyPair>) {
        keyPairs.forEach {
            if (it.dataKey != null && it.metadataKey == null) {
                logger.error("Metadata file not found for the data file ${it.dataKey}")
                throw RuntimeException("Metadata file not found for the data file ${it.dataKey}")
            }
            else if (it.metadataKey != null && it.dataKey == null) {
                logger.error("Data file not found for the metadata file ${it.metadataKey}")
            }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(KeyPairGenerator::class.toString())
    }
}



