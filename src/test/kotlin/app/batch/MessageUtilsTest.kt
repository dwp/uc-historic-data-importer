package app.batch

import com.beust.klaxon.JsonObject
import com.beust.klaxon.KlaxonException
import io.kotlintest.matchers.beInstanceOf
import io.kotlintest.properties.assertAll
import io.kotlintest.should
import io.kotlintest.shouldBe
import io.kotlintest.shouldNotBe
import io.kotlintest.shouldThrow
import org.junit.Test
import java.text.ParseException
import com.nhaarman.mockitokotlin2.*


class MessageUtilsTest {

    val messageUtils = MessageUtils()

    @Test
    fun valid_input_converts_to_json() {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = messageUtils.parseJson(jsonString)

        json should beInstanceOf<JsonObject>()
        json.string("testOne") shouldBe "test1"
        json.int("testTwo") shouldBe 2
    }

    @Test
    fun valid_nested_input_converts_to_json() {
        val jsonString = "{\"testOne\":{\"testTwo\":2}}"
        val json: JsonObject = messageUtils.parseJson(jsonString)
        val jsonTwo: JsonObject = json.obj("testOne") as JsonObject

        json should beInstanceOf<JsonObject>()
        jsonTwo.int("testTwo") shouldBe 2
    }

    @Test
    fun invalid_nested_input_throws_exception() {
        val jsonString = "{\"testOne\":"

        shouldThrow<KlaxonException> {
            messageUtils.parseJson(jsonString)
        }
    }

    @Test
    fun sorts_json_by_key_name() {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonObjectUnsorted: JsonObject = messageUtils.parseJson(jsonStringUnsorted)
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = messageUtils.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun sorts_json_by_key_name_case_sensitively() {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonObjectUnsorted: JsonObject = messageUtils.parseJson(jsonStringUnsorted)
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = messageUtils.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun checksums_are_different_with_different_inputs() {
        val jsonStringOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonStringTwo = "{\"testOne\":\"test2\", \"testTwo\":2}"
        val checksum = messageUtils.generateFourByteChecksum(jsonStringOne)
        val checksumTwo = messageUtils.generateFourByteChecksum(jsonStringTwo)

        checksum shouldNotBe checksumTwo
    }

    @Test
    fun can_generate_consistent_checksums_from_json() {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = messageUtils.parseJson(jsonString)
        val checksumOne = messageUtils.generateFourByteChecksum(json.toString())
        val checksumTwo = messageUtils.generateFourByteChecksum(json.toString())

        checksumOne shouldBe checksumTwo
    }

    @Test
    fun generated_checksums_are_four_bytes() {
        assertAll { input: String ->
            val checksum = messageUtils.generateFourByteChecksum(input)
            checksum.size shouldBe 4
        }
    }

    @Test
    fun valid_timestamp_format_in_the_message_gets_parsed_as_long_correctly() {
        val timeStampAsLong = messageUtils.getTimestampAsLong("2018-12-14T15:01:02.000Z")
        timeStampAsLong shouldBe 1544799662000
    }

    @Test
    fun Invalid_timestamp_format_in_the_message_throws_Exception() {
        shouldThrow<ParseException> {
            messageUtils.getTimestampAsLong("2018-12-14")
        }
    }

    @Test
    fun valid_dates_without_timezone_parses_without_exception() {
        1544799662000L shouldBe messageUtils.getTimestampAsLong("2018-12-14T15:01:02.000Z")
    }

    @Test
    fun valid_date_with_timezone_parses_without_exception() {
        1544799662000L shouldBe messageUtils.getTimestampAsLong("2018-12-14T15:01:02.000+0000")
    }

    @Test
    fun Invalid_json_with_lastModifiedDateTime_attribute_value_as_empty_throws_Exception() {
        shouldThrow<ParseException> {
            messageUtils.getTimestampAsLong("")
        }
    }

    @Test
    fun Invalid_json_with_lastModifiedDateTime_attribute_value_as_blank_throws_Exception() {
        shouldThrow<ParseException> {
            messageUtils.getTimestampAsLong("   ")
        }
    }

    @Test
    fun generated_keys_are_consistent_for_identical_inputs() {

        val json: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")

        val keyOne: ByteArray = messageUtils.generateKey(json)
        val keyTwo: ByteArray = messageUtils.generateKey(json)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generated_keys_are_different_for_different_inputs() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":3}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generated_keys_are_consistent_for_identical_inputs_regardless_of_order() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testTwo\":2, \"testOne\":\"test1\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generated_keys_are_consistent_for_identical_inputs_regardless_of_whitespace() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{    \"testOne\":              \"test1\",            \"testTwo\":  2}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generated_keys_are_consistent_for_identical_inputs_regardless_of_order_and_whitespace() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{    \"testTwo\":              2,            \"testOne\":  \"test1\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generated_keys_will_vary_given_values_with_different_whitespace() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test 1\", \"testTwo\":2}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generated_keys_will_vary_given_values_that_are_string_and_int_in_each_input() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":\"2\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generated_keys_will_vary_given_values_that_are_string_and_float_in_each_input() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2.0}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":\"2.0\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generated_keys_will_vary_given_values_that_are_string_and_boolean_in_each_input() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":false}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":\"false\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generated_keys_will_vary_given_values_that_are_string_and_null_in_each_input() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":null}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":\"null\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun id_is_returned_from_valid_json() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":{\"test_key\":\"test_value\"}}}")
        val idString = "{\"test_key\":\"test_value\"}"

        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson?.toJsonString() shouldBe idString
    }

    @Test
    fun null_is_returned_from_json_where_message_does_not_exist() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"test_object\":{\"_id\":{\"test_key\":\"test_value\"}}}")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson shouldBe null
    }

    @Test
    fun null_is_returned_from_json_where_message_is_not_an_object() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":\"test_value\"}")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson shouldBe null
    }

    @Test
    fun null_is_returned_from_json_where_id_is_missing() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"test_object\":{\"test_key\":\"test_value\"}}}")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson shouldBe null
    }


    @Test
    fun altered_id_is_returned_from_json_where_id_is_not_an_object() {

        val jsonOne: JsonObject = messageUtils.parseJson("""{"message":{"_id":"test_value"}}""")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)
        val expected = JsonObject(mutableMapOf("id" to "test_value"))
        idJson shouldBe expected
    }

    @Test
    fun generated_key_is_consistent_for_identical_record_body_independant_of_key_order_and_whitespace() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":{\"test_key_a\":\"test_value_a\",\"test_key_b\"    :\"test_value_b\"}}}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":{\"test_key_b\":     \"test_value_b\",\"test_key_a\":\"test_value_a\"}}}")

        val keyOne: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKeyFromRecordBody(jsonTwo)

        keyOne shouldNotBe ByteArray(0)
        keyOne shouldBe keyTwo
    }


    @Test
    fun empty_is_returned_from_record_body_key_generation_where_message_does_not_exist() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"test_object\":{\"_id\":{\"test_key\":\"test_value\"}}}")
        val key: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    @Test
    fun empty_is_returned_from_record_body_key_generation_where_message_is_not_an_object() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":\"test_value\"}")
        val key: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    @Test
    fun empty_is_returned_from_record_body_key_generation_where_id_is_missing() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"test_object\":{\"test_key\":\"test_value\"}}}")
        val key: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    @Test
    fun empty_is_returned_from_record_body_key_generation_where_id_is_not_an_object() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":\"test_value\"}}")
        val key: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)
        val noChecksum = String(key).substring(3)
        val expected = """{"id":"test_value"}"""
        noChecksum shouldBe expected
    }

    @Test
    fun type_is_returned_from_valid_input() {
        val jsonString = """{
            "message": {
               "@type": "MONGO_IMPORT_TWO",
            }
        }"""

        val json: JsonObject = messageUtils.parseJson(jsonString)
        messageUtils.getType(json) shouldBe "MONGO_IMPORT_TWO"
    }

    @Test
    fun default_type_is_returned_from_invalid_input() {
        val jsonString = """{
            "message": {
               "notype": "HDI",
            }
        }"""

        val json: JsonObject = messageUtils.parseJson(jsonString)
        messageUtils.getType(json) shouldBe messageUtils.typeDefault
    }

    @Test
    fun should_sort_json_by_key_name() {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = messageUtils.sortJsonStringByKey(jsonStringUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun should_sort_json_by_key_name_for_single_key() {
        val jsonStringUnsorted = "{\"\$oid\":\"test1\"}"
        val jsonStringSorted = "{\"\$oid\":\"test1\"}"

        val sortedJson = messageUtils.sortJsonStringByKey(jsonStringUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun should_sort_json_by_key_name_case_sensitively() {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = messageUtils.sortJsonStringByKey(jsonStringUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun testGetVersionUsesLastModifiedWhenMongoImport() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_IMPORT"
        val removedDate = "2000-01-01T00:00:00.000Z"
        val archivedDate = "2005-01-01T00:00:00.000Z"
        val expected = 978307200000L
        given(messageUtils.getTimestampAsLong(lastModifiedDate)).willReturn(expected)
        val actual = messageUtils.getVersion(innerType, lastModifiedDate, removedDate,
                archivedDate)
        assertEquals(expected, actual)
        verify(messageUtils, times(1)).getTimestampAsLong(lastModifiedDate)
    }

    @Test
    fun testGetVersionUsesLastModifiedWhenMongoInsert() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_INSERT"
        val removedDate = "2000-01-01T00:00:00.000Z"
        val archivedDate = "2005-01-01T00:00:00.000Z"
        val expected = 978307200000L
        given(messageUtils.getTimestampAsLong(lastModifiedDate)).willReturn(expected)
        val actual = messageUtils.getVersion(innerType, lastModifiedDate, removedDate,
                archivedDate)
        assertEquals(expected, actual)
        verify(messageUtils, times(1)).getTimestampAsLong(lastModifiedDate)
    }

    @Test
    fun testGetVersionToleratesGarbageWhenNotDelete() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_IMPORT"
        val removedDate = "NON PARSEABLE REMOVED DATE"
        val archivedDate = "NON PARSEABLE ARCHIVED DATE"
        val expected = 978307200000L
        given(messageUtils.getTimestampAsLong(lastModifiedDate)).willReturn(expected)
        val actual = messageUtils.getVersion(innerType, lastModifiedDate,
                removedDate, archivedDate)
        assertEquals(expected, actual)
        verify(messageUtils, times(1)).getTimestampAsLong(lastModifiedDate)
    }

    @Test
    fun testGetVersionUsesRemovedDateWhenMongoDelete() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_DELETE"
        val removedDate = "2000-01-01T00:00:00.000Z"
        val archivedDate = "2005-01-01T00:00:00.000Z"

        val expected = 946684800000L
        given(messageUtils.getTimestampAsLong(removedDate)).willReturn(expected)
        val actual = messageUtils.getVersion(innerType, lastModifiedDate, removedDate,
                archivedDate)
        assertEquals(expected, actual)
        verify(messageUtils, times(1)).getTimestampAsLong(removedDate)
    }

    @Test
    fun testGetVersionUsesArchivedDateWhenMongoDeleteButEmptyRemovedDate() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_DELETE"
        val removedDate = ""
        val archivedDate = "2005-01-01T00:00:00.000Z"
        val expected = 1104537600000L
        given(messageUtils.getTimestampAsLong(archivedDate)).willReturn(expected)

        val actual = messageUtils.getVersion(innerType, lastModifiedDate,
                removedDate, archivedDate)
        assertEquals(expected, actual)
        verify(messageUtils, times(1)).getTimestampAsLong(archivedDate)
    }

    @Test
    fun testGetVersionUsesLastModifiedWhenMongoDeleteButEmptyRemovedDateAndArchivedDate() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_DELETE"
        val removedDate = ""
        val archivedDate = ""
        val expected = 978307200000L
        given(messageUtils.getTimestampAsLong(lastModifiedDate)).willReturn(expected)

        val actual = messageUtils.getVersion(innerType, lastModifiedDate,
                removedDate, archivedDate)
        assertEquals(expected, actual)
        verify(messageUtils, times(1)).getTimestampAsLong(lastModifiedDate)
    }

    @Test
    fun testGetVersionUsesLastModifiedWhenMongoDeleteButUnparseableRemovedDate() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_DELETE"
        val removedDate = "BADLY_FORMATTED_DATE"
        val archivedDate = "2005-01-01T00:00:00.000Z"
        val expected = 978307200000L
        given(messageUtils.getTimestampAsLong(removedDate)).willThrow(ParseException("BAD DATE", 10))
        given(messageUtils.getTimestampAsLong(lastModifiedDate)).willReturn(expected)

        val actual = messageUtils.getVersion(innerType, lastModifiedDate,
                removedDate, archivedDate)
        assertEquals(expected, actual)
        verify(messageUtils, times(1)).getTimestampAsLong(removedDate)
        verify(messageUtils, times(1)).getTimestampAsLong(lastModifiedDate)
    }

    @Test
    fun testGetVersionUsesLastModifiedWhenMongoDeleteAndEmptyRemovedDateAndUnparseableArchivedDate() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_DELETE"
        val removedDate = ""
        val archivedDate = "BADLY_FORMATTED_DATE"
        val expected = 978307200000L
        given(messageUtils.getTimestampAsLong(archivedDate)).willThrow(ParseException("BAD ARCHIVE DATE", 10))
        given(messageUtils.getTimestampAsLong(lastModifiedDate)).willReturn(expected)

        val actual = messageUtils.getVersion(innerType, lastModifiedDate,
                removedDate, archivedDate)
        assertEquals(expected, actual)
        verify(messageUtils, times(1)).getTimestampAsLong(archivedDate)
        verify(messageUtils, times(1)).getTimestampAsLong(lastModifiedDate)
    }
}
