package app.services.impl

import app.batch.HbaseClient
import app.services.FilterService
import org.apache.commons.lang3.StringUtils
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.text.SimpleDateFormat

@Service
class FilterServiceImpl(private val hbase: HbaseClient) : FilterService {

    override fun filterStatus(tableName: String, key: ByteArray, timestamp: Long): FilterService.FilterStatus =
            when {
                // timestamp == epoch means a record with no last modified date
                // so put these in as a precaution as they may be recent.
                timestamp < earlierThan && timestamp != epoch -> {
                    FilterService.FilterStatus.FilterTooEarly
                }
                timestamp > laterThan -> {
                    FilterService.FilterStatus.FilterTooLate
                }
                !skipExisting -> {
                    FilterService.FilterStatus.DoNotFilter
                }
                else -> {
                    if (hbase.exists(tableName, key, timestamp)) {
                        FilterService.FilterStatus.FilterExists
                    }
                    else {
                        FilterService.FilterStatus.DoNotFilter
                    }
                }
            }

    private val skipExisting: Boolean by lazy {
        skipExistingRecords.toBoolean()
    }

    private val earlierThan: Long by lazy {
        if (StringUtils.isNotBlank(skipEarlierThan)) {
            if (alternateDateFormatPattern.matches(skipEarlierThan)) {
                SimpleDateFormat(alternateDateFormat).parse(skipEarlierThan).time
            }
            else {
                SimpleDateFormat(dateFormat).parse(skipEarlierThan).time
            }
        }
        else {
            Long.MIN_VALUE
        }
    }

    private val laterThan: Long by lazy {
        if (StringUtils.isNotBlank(skipLaterThan)) {
            if (alternateDateFormatPattern.matches(skipLaterThan)) {
                SimpleDateFormat(alternateDateFormat).parse(skipLaterThan).time
            }
            else {
                SimpleDateFormat(dateFormat).parse(skipLaterThan).time
            }
        }
        else {
            Long.MAX_VALUE
        }
    }

    @Value("\${skip.existing.records:true}")
    private lateinit var skipExistingRecords: String

    @Value("\${skip.earlier.than:}")
    private lateinit var skipEarlierThan: String

    @Value("\${skip.later.than:}")
    private lateinit var skipLaterThan: String

    companion object {
        const val dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        const val alternateDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        val epoch = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ").parse("1980-01-01T00:00:00.000+0000").time
        val alternateDateFormatPattern = Regex("""Z$""")
    }
}
