package app.services.impl

import app.batch.HbaseClient
import app.services.FilterService
import com.nhaarman.mockitokotlin2.*
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.mock.mockito.MockBean
import org.springframework.test.context.junit4.SpringRunner
import org.springframework.test.util.ReflectionTestUtils

@RunWith(SpringRunner::class)
@SpringBootTest(classes = [FilterServiceImpl::class])
class FilterServiceImplTest {

    @Autowired
    private lateinit var filterService: FilterService

    @MockBean
    private lateinit var hbaseClient: HbaseClient

    @Before
    fun before() {
        reset(hbaseClient)
    }

    @Test
    fun testDoesNotFilterIfSkipExistingFalse() {
        val doPut = filterService.putRecord(tableName, recordId, timestamp)
        assertTrue(doPut)
        verifyZeroInteractions(hbaseClient)
    }

    @Test
    fun testFiltersIfSkipExistingTrueAndRecordExists() {
        ReflectionTestUtils.setField(filterService, "skipExistingRecords", "true")
        val doPut = filterService.putRecord(tableName, recordId, timestamp)
        given(hbaseClient.exists(tableName, recordId, timestamp)).willReturn(true)
        assertFalse(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
        verifyNoMoreInteractions(hbaseClient)
    }

    @Test
    fun testDoesNotFilterIfSkipExistingTrueAndRecordDoesNotExist() {
        ReflectionTestUtils.setField(filterService, "skipExistingRecords", "true")
        val doPut = filterService.putRecord(tableName, recordId, timestamp)
        given(hbaseClient.exists(tableName, recordId, timestamp)).willReturn(false)
        assertFalse(doPut)
        verify(hbaseClient, times(1)).exists(tableName, recordId, timestamp)
        verifyNoMoreInteractions(hbaseClient)
    }

    companion object {
        const val tableName = "table"
        val recordId = "recordId".toByteArray()
        const val timestamp = 100L
    }
}
