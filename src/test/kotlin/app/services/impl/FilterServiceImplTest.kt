package app.services.impl

import app.batch.HbaseClient
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import org.springframework.test.util.ReflectionTestUtils
import java.text.SimpleDateFormat

class FilterServiceImplTest {

    @Test
    fun testDoesNotPutIfTimestampBeforeEarlierThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient, skipEarlierThan = "2020-10-01T12:00:00.000")
        val tooEarlyTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T11:59:59.999").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, tooEarlyTimestamp)
        assertFalse(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampEqualToEarlierThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient, skipEarlierThan = "2020-10-01T12:00:00.000")
        val tooEarlyTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, tooEarlyTimestamp)
        assertFalse(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampGreaterThanEarlierThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient, skipEarlierThan = "2020-10-01T12:00:00.000")
        val okTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, okTimestamp)
        assertTrue(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampAfterLaterThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient, skipLaterThan = "2020-10-01T12:00:00.000")
        val tooLateTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, tooLateTimestamp)
        assertFalse(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampEqualToLaterThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient, skipLaterThan = "2020-10-01T12:00:00.000")
        val tooEarlyTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, tooEarlyTimestamp)
        assertFalse(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampEarlierThanLaterThanFilterAndFilterExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient, skipLaterThan = "2020-10-01T12:00:00.001")
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertTrue(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampBeforeEarlierThanFilterAndFilterExistingTrue() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val tooEarlyTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T11:59:59.999").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, tooEarlyTimestamp)
        assertFalse(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampEqualToEarlierThanFilterAndFilterExistingTrue() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val tooEarlyTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, tooEarlyTimestamp)
        assertFalse(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampGreaterThanEarlierThanFilterAndFilterExistingTrueAndRecordDoesNotExist() {
        val okTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, okTimestamp) } doReturn false
        }
        var filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val doPut = filterService.shouldPutRecord(tableName, recordId, okTimestamp)
        assertTrue(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, okTimestamp)
    }

    @Test
    fun testDoesNotPutIfTimestampGreaterThanEarlierThanFilterAndFilterExistingTrueAndRecordDoesExist() {
        val okTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, okTimestamp) } doReturn true
        }
        var filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val doPut = filterService.shouldPutRecord(tableName, recordId, okTimestamp)
        assertFalse(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, okTimestamp)
    }

    @Test
    fun testDoesNotPutIfTimestampAfterLaterThanFilterAndFilterExistingTrue() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val tooLateTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.001").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, tooLateTimestamp)
        assertFalse(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfTimestampEqualToLaterThanFilterAndFilterExistingTrue() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.000",
                skipExistingRecords = "true")
        val tooEarlyTimestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time
        val doPut = filterService.shouldPutRecord(tableName, recordId, tooEarlyTimestamp)
        assertFalse(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfTimestampEarlierThanLaterThanFilterAndFilterExistingTrueAndRecordDoesNotExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn false
        }

        var filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "true")
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertTrue(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesNotPutIfTimestampEarlierThanLaterThanFilterAndFilterExistingTrueAndRecordDoesExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn true
        }

        var filterService = filterService(hbaseClient,
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "true")
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertFalse(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesNotPutIfTimestampBetweenFiltersAndFilterExistingTrueAndRecordDoesExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn true
        }

        var filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T11:59:59.999",
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "true")
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertFalse(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesPutIfTimestampBetweenFiltersAndFilterExistingTrueAndRecordDoesNotExist() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn false
        }

        var filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T11:59:59.999",
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "true")
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertTrue(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
    }

    @Test
    fun testDoesPutIfTimestampBetweenFiltersAndFilterExistingFalse() {
        val timestamp = SimpleDateFormat(FilterServiceImpl.dateFormat)
                .parse("2020-10-01T12:00:00.000").time

        val hbaseClient = mock<HbaseClient>()

        var filterService = filterService(hbaseClient,
                skipEarlierThan = "2020-10-01T11:59:59.999",
                skipLaterThan = "2020-10-01T12:00:00.001",
                skipExistingRecords = "false")
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertTrue(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfSkipExistingFalse() {
        val hbaseClient = mock<HbaseClient>()
        var filterService = filterService(hbaseClient)
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertTrue(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotPutIfSkipExistingTrueAndRecordExists() {
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn true
        }
        var filterService = filterService(hbaseClient, skipExistingRecords = "true")
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertFalse(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
        verifyNoMoreInteractions(hbaseClient)
    }

    @Test
    fun testDoesPutIfSkipExistingTrueAndRecordDoesNotExist() {
        val hbaseClient = mock<HbaseClient> {
            on { exists(tableName, recordId, timestamp) } doReturn false
        }
        var filterService = filterService(hbaseClient, skipExistingRecords = "true")
        val doPut = filterService.shouldPutRecord(tableName, recordId, timestamp)
        assertTrue(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
        verifyNoMoreInteractions(hbaseClient)
    }


    private fun filterService(hbaseClient: HbaseClient,
                              skipEarlierThan: String = "",
                              skipLaterThan: String = "",
                              skipExistingRecords: String = "false")
         = FilterServiceImpl(hbaseClient).also {
                    ReflectionTestUtils.setField(it, "skipEarlierThan", skipEarlierThan)
                    ReflectionTestUtils.setField(it, "skipLaterThan", skipLaterThan)
                    ReflectionTestUtils.setField(it, "skipExistingRecords", skipExistingRecords)
                }

    companion object {
        const val tableName = "table"
        val recordId = "recordId".toByteArray()
        const val timestamp = 100L
    }
}
