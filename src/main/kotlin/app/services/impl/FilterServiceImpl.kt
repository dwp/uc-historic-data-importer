package app.services.impl

import app.batch.HbaseClient
import app.services.FilterService
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service

@Service
class FilterServiceImpl(private val hbase: HbaseClient): FilterService {

    override fun putRecord(tableName: String, key: ByteArray, timestamp: Long): Boolean {
        if (!skipExistingRecords.toBoolean()) {
            return true
        }

        return hbase.exists(tableName, key, timestamp)
    }

    @Value("\${skip.existing.records:false}")
    private lateinit var skipExistingRecords: String
}
