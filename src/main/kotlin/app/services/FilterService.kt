package app.services

interface FilterService {

    fun filterStatus(tableName: String, key: ByteArray, timestamp: Long): FilterStatus

    enum class FilterStatus {
        DoNotFilter, FilterTooEarly, FilterTooLate, FilterExists
    }

}
