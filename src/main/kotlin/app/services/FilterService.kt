package app.services

interface FilterService {
    fun shouldPutRecord(tableName: String, key: ByteArray, timestamp: Long): Boolean
}
