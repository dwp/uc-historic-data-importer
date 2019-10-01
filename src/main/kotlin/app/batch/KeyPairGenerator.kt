package app.batch

import org.springframework.stereotype.Component

@Component
class KeyPairGenerator {
    // Function to group by all given keys by the given regex
    fun generateKeyPair(keys: List<String>, regex: Regex, dataExtension: String, metadataExtension: String): List<KeyPair> {
        val regex = "^[A-Za-z]*\\.[A-Za-z0-9]*\\.[0-9]{4}".toRegex()
        val keys = listOf("adb.collection1.0000.json.gz.enc", "cdb.collection1.0000.json.gz.enc",
            "adb.collection1.0000.json.gz.encryption.json", "cdb.collection1.0000.json.gz.encryption.json",
            "sdb1.collection1.0000.json.gz.enc", "sdb1.collection1.0000.json.gz.encryption.json", "abc.json")

        val map = keys.groupBy { regex.find(it)?.value }
        return validate(map, dataExtension, metadataExtension)
    }


    // function to validate if there is any null key that doesnt comply to the given regex
// also validates if there are more than a pair for a given regex
    fun validate(map: Map<String?, List<String>>, dataExtension: String, metadataExtension: String): List<KeyPair> {
        val dataExtension = ".enc"
        val metadataExtension = ".encryption.json"
        val nullValuesMap = map.filterKeys { it == null }
        // log info to print the size of the values
        // log debug to print all the values
        val moreMap = map.filterValues { it.size > 2 }
        // log  info to tell more than 2 of the same format, hence ambuiguity
        // log  info to show all the values
        // is it possible in s3
        val lessMap = map.filterValues { it.size <= 2 }
        val hh = lessMap.mapValues {
            val keyPair: KeyPair = KeyPair(null, null)
            // TODO check how immutability can be enforced here
            it.value.map {
                if (it.contains(dataExtension)) {
                    keyPair.dataKey = it
                } else if (it.contains(metadataExtension)) {
                    keyPair.metadataKey = it
                }
            }
            keyPair
        }
        validateDataPairs(hh)
        return hh.filterValues { it.dataKey !== null && it.metadataKey != null }.map { it.value }
    }

    fun validateDataPairs(map: Map<String?, KeyPair>) {
        val dataPairs = map.values
        dataPairs.forEach {
            if (it.dataKey != null && it.metadataKey == null)
            //log error
                throw RuntimeException("Unacceptable")
            else if (it.metadataKey != null && it.dataKey == null) {
                // log  warning
            } else {
                // log both data and metadata not available
            }
        }
    }
}



