package app.services.impl

import app.batch.HbaseClient
import app.services.FilterService
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.text.SimpleDateFormat

@Service
class FilterServiceImpl(private val hbase: HbaseClient) : FilterService {

    override fun shouldPutRecord(tableName: String, key: ByteArray, timestamp: Long): Boolean {

        if (timestamp <= earlierThan) {
            return false
        }

        if (timestamp >= laterThan) {
            return false
        }

        if (!skipExisting) {
            return true
        }

        return !hbase.exists(tableName, key, timestamp)
    }


    private val skipExisting: Boolean by lazy {
        skipExistingRecords.toBoolean()
    }

    private val earlierThan: Long by lazy {
        if (StringUtils.isNotBlank(skipEarlierThan)) {
            SimpleDateFormat(dateFormat).parse(skipEarlierThan).time
        }
        else {
            Long.MIN_VALUE
        }
    }

    private val laterThan: Long by lazy {
        if (StringUtils.isNotBlank(skipLaterThan)) {
            SimpleDateFormat(dateFormat).parse(skipLaterThan).time
        }
        else {
            Long.MAX_VALUE
        }
    }

    @Value("\${skip.existing.records:false}")
    private lateinit var skipExistingRecords: String

    @Value("\${skip.earlier.than:}")
    private lateinit var skipEarlierThan: String

    @Value("\${skip.later.than:}")
    private lateinit var skipLaterThan: String

    companion object {
        const val dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
    }
}
