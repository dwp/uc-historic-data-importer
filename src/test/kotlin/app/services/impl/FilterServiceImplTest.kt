package app.services.impl

import app.batch.HBaseWriter
import app.batch.HbaseClient
import app.services.FilterService
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.assertEquals
import org.junit.Test
import org.springframework.test.util.ReflectionTestUtils
import java.text.SimpleDateFormat

class FilterServiceImplTest {

    @Test
    fun testDoesNotPutIfTimestampBeforeEarlierThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient, skipEarlierThan = "2020-10-01T12:00:00.000")
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T11:59:59.999").time
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.FilterTooEarly, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampEqualToEarlierThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient,
                skipExistingRecords = "false",
                skipEarlierThan = "2020-10-01T12:00:00.000")
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampIsEpochAndLessThanEarlierThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000", skipExistingRecords = "false")
        val timestamp = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
                .parse(HBaseWriter.EPOCH).time
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampIsEpochAndLessThanEarlierThanFilterAndFilterExistingTrueAndRecordExists() {
        val timestamp = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
                .parse(HBaseWriter.EPOCH).time
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp)} doReturn true
        }
        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000", skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.FilterExists, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesPutIfTimestampIsEpochAndLessThanEarlierThanFilterAndFilterExistingTrueAndRecordDoesNotExist() {
        val timestamp = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
                .parse(HBaseWriter.EPOCH).time
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp)} doReturn false
        }
        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000", skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }


    @Test
    fun testDoesPutIfTimestampGreaterThanEarlierThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000", skipExistingRecords = "false")
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampAfterLaterThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient, skipLaterThan = "2020-10-01T12:00:00.000")
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.FilterTooLate, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampEqualToLaterThanFilterAndFilterExistingFalse() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient,
                skipExistingRecords = "false",
                skipLaterThan = "2020-10-01T12:00:00.000")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampEarlierThanLaterThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "false")
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampBeforeEarlierThanFilterAndFilterExistingTrue() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T11:59:59.999").time
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.FilterTooEarly, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampEqualToEarlierThanFilterAndFilterExistingTrue() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn false
        }
        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesPutIfTimestampGreaterThanEarlierThanFilterAndFilterExistingTrueAndRecordDoesNotExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn false
        }
        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesNotPutIfTimestampGreaterThanEarlierThanFilterAndFilterExistingTrueAndRecordDoesExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn true
        }
        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.FilterExists, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesNotPutIfTimestampAfterLaterThanFilterAndFilterExistingTrue() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.FilterTooLate, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampEqualToLaterThanFilterAndFilterExistingTrue() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn false
        }
        val filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesPutIfTimestampEarlierThanLaterThanFilterAndFilterExistingTrueAndRecordDoesNotExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn false
        }

        val filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesNotPutIfTimestampEarlierThanLaterThanFilterAndFilterExistingTrueAndRecordDoesExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn true
        }

        val filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.FilterExists, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesNotPutIfTimestampBetweenFiltersAndFilterExistingTrueAndRecordDoesExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn true
        }

        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T11:59:59.999",
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.FilterExists, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesPutIfTimestampBetweenFiltersAndFilterExistingTrueAndRecordDoesNotExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn false
        }

        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T11:59:59.999",
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesPutIfTimestampBetweenFiltersAndFilterExistingFalse() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient>()

        val filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T11:59:59.999",
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "false")
        val filterStatus = filterService.filterStatus(tableName, recordId, timestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfSkipExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        val filterService = filterService(hbaseClient, skipExistingRecords = "false")
        val filterStatus = filterService.filterStatus(tableName, recordId, defaultTimestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfSkipExistingTrueAndRecordExists() {
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, defaultTimestamp) } doReturn true
        }
        val filterService = filterService(hbaseClient, skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, defaultTimestamp)
        assertEquals(FilterService.FilterStatus.FilterExists, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, defaultTimestamp)
        verifyNoMoreInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfSkipExistingTrueAndRecordDoesNotExist() {
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, defaultTimestamp) } doReturn false
        }
        val filterService = filterService(hbaseClient, skipExistingRecords = "true")
        val filterStatus = filterService.filterStatus(tableName, recordId, defaultTimestamp)
        assertEquals(FilterService.FilterStatus.DoNotFilter, filterStatus)
        verify(hbaseClient, times(1)).exists(tableName, recordId, defaultTimestamp)
        verifyNoMoreInteractions(hbaseClient)
    }


    private fun filterService(hbaseClient: HbaseClient,
                              skipEarlierThan: String = "",
                              skipLaterThan: String = "",
                              skipExistingRecords: String = "true")
         = FilterServiceImpl(hbaseClient).also {
                    ReflectionTestUtils.setField(it, "skipEarlierThan", skipEarlierThan)
                    ReflectionTestUtils.setField(it, "skipLaterThan", skipLaterThan)
                    ReflectionTestUtils.setField(it, "skipExistingRecords", skipExistingRecords)
                }

    companion object {
        const val tableName = "table"
        val recordId = "recordId".toByteArray()
        const val defaultTimestamp = 100L
    }
}