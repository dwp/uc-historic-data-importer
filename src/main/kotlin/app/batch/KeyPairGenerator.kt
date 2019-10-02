package app.batch

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

@Component
class KeyPairGenerator {
    fun generateKeyPairs(keys: List<String>, regex: Regex, dataExtension: Regex, metadataExtension: Regex): List<KeyPair> {
        val keysMap = keys.groupBy { regex.find(it)?.value }
        val (nullValuesMap, lessMap) = keysMap.map { it }.partition { it.key == null }
        logger.warn("${nullValuesMap.flatMap { it.value }.count()} found that dont match the file regex")
        nullValuesMap.forEach { logger.info("${it.value}") }
        val hh = lessMap.map {
            logger.info("${it.key} ${it.value}")
            val notRecognised = it.value.filterNot { abc -> (abc.contains(dataExtension) || abc.contains(metadataExtension)) }
            logger.info("${notRecognised}")
            val dataKey = it.value.find { it.contains(dataExtension) }
            val metadatakey = it.value.find { it.contains(metadataExtension) }
            KeyPair(dataKey, metadatakey)
        }
        validateKeyPairs(hh)
        return hh.filter { it.dataKey !== null && it.metadataKey != null }
    }

    fun validateKeyPairs(keyPairs: List<KeyPair>) {
        keyPairs.forEach {
            if (it.dataKey != null && it.metadataKey == null) {
                logger.error("Metadata file not found for the data file ${it.dataKey}")
                throw RuntimeException("Metadata file not found for the data file ${it.dataKey}")
            } else if (it.metadataKey != null && it.dataKey == null) {
                logger.error("Data file not found for the metadata file ${it.metadataKey}")
            }
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(KeyPairGenerator::class.toString())
    }
}



