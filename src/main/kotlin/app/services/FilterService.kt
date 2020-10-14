package app.services

import app.domain.HBaseRecord

interface FilterService {

    fun nonExistent(tableName: String, records: List<HBaseRecord>): List<HBaseRecord>

    fun filterStatus(tableName: String, key: ByteArray, timestamp: Long): FilterStatus

    enum class FilterStatus {
        DoNotFilter, FilterTooEarly, FilterTooLate, FilterExists
    }

}
