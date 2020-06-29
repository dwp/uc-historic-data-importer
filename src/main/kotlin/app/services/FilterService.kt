package app.services

interface FilterService {
    fun putRecord(tableName: String, key: ByteArray, timestamp: Long): Boolean
}
