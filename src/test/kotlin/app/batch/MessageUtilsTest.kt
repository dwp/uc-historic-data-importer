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
import java.text.SimpleDateFormat
import java.util.*


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
        val jsonString = "{\n" +
                "        \"traceId\": \"00001111-abcd-4567-1234-1234567890ab\",\n" +
                "        \"unitOfWorkId\": \"00002222-abcd-4567-1234-1234567890ab\",\n" +
                "        \"@type\": \"V4\",\n" +
                "        \"version\": \"core-X.release_XXX.XX\",\n" +
                "        \"timestamp\": \"2018-12-14T15:01:02.000+0000\",\n" +
                "        \"message\": {\n" +
                "            \"@type\": \"MONGO_UPDATE\",\n" +
                "            \"collection\": \"exampleCollectionName\",\n" +
                "            \"db\": \"exampleDbName\",\n" +
                "            \"_id\": {\n" +
                "                \"exampleId\": \"aaaa1111-abcd-4567-1234-1234567890ab\"\n" +
                "            },\n" +
                "           \"_lastModifiedDateTime\": {\"\$date\" : \"2018-12-14T15:01:02.000+0000\"},\n" +
                "            \"encryption\": {\n" +
                "                \"encryptionKeyId\": \"55556666-abcd-89ab-1234-1234567890ab\",\n" +
                "                \"encryptedEncryptionKey\": \"bHJjhg2Jb0uyidkl867gtFkjl4fgh9Ab\",\n" +
                "                \"initialisationVector\": \"kjGyvY67jhJHVdo2\",\n" +
                "                \"keyEncryptionKeyId\": \"example-key_2019-12-14_01\"\n" +
                "            },\n" +
                "            \"dbObject\": \"bubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9AbubHJjhg2Jb0uyidkl867gtFkjl4fgh9A\"\n" +
                "        }\n" +
                "    }"

        val json: JsonObject = messageUtils.parseJson(jsonString)
        val timestamp = messageUtils.getLastModifiedTimestamp(json)
        val timeStampAsLong = messageUtils.getTimestampAsLong(timestamp)
        timestamp shouldBe "2018-12-14T15:01:02.000+0000"
        timeStampAsLong shouldBe 1544799662000
    }

    @Test
    fun Invalid_timestamp_format_in_the_message_throws_Exception() {
        val jsonString = "{\n" +
                "        \"message\": {\n" +
                "           \"_lastModifiedDateTime\": {\"\$date\" : \"2018-12-14\"},\n" +
                "        }\n" +
                "    }"

        val json: JsonObject = messageUtils.parseJson(jsonString)
        val timestamp = messageUtils.getLastModifiedTimestamp(json)
        timestamp shouldBe "2018-12-14"
        shouldThrow<ParseException> {
            messageUtils.getTimestampAsLong(timestamp)
        }
    }

    @Test
    fun Invalid_json_with_missing_message_attribute_throws_Exception() {
        val jsonString = "{\n" +
                "           \"_lastModifiedDateTime1\": {\"\$date\" : \"2018-12-14T15:01:02.000+0000\"},\n" +
                "    }"

        val json: JsonObject = messageUtils.parseJson(jsonString)
        shouldThrow<Exception> {
            val lastModifiedTimestamp = messageUtils.getLastModifiedTimestamp(json)
            messageUtils.getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    @Test
    fun Invalid_json_with_missing_lastModifiedDateTime_attribute_throws_Exception() {
        val jsonString = "{\n" +
                "        \"message\": {\n" +
                "           \"_lastModifiedDateTime1\": \"2018-12-14T15:01:02.000+0000\",\n" +
                "        }\n" +
                "    }"

        val json: JsonObject = messageUtils.parseJson(jsonString)
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = messageUtils.getLastModifiedTimestamp(json)
            messageUtils.getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    @Test
    fun Invalid_json_with_lastModifiedDateTime_attribute_value_as_empty_throws_Exception() {
        val jsonString = "{\n" +
                "        \"message\": {\n" +
                "           \"_lastModifiedDateTime\": \"\",\n" +
                "        }\n" +
                "    }"

        val json: JsonObject = messageUtils.parseJson(jsonString)
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = messageUtils.getLastModifiedTimestamp(json)
            messageUtils.getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    @Test
    fun Invalid_json_with_lastModifiedDateTime_attribute_value_as_blank_throws_Exception() {
        val jsonString = "{\n" +
                "        \"message\": {\n" +
                "           \"_lastModifiedDateTime\": \"   \",\n" +
                "        }\n" +
                "    }"

        val json: JsonObject = messageUtils.parseJson(jsonString)
        shouldThrow<RuntimeException> {
            val lastModifiedTimestamp = messageUtils.getLastModifiedTimestamp(json)
            messageUtils.getTimestampAsLong(lastModifiedTimestamp)
        }
    }

    @Test
    fun Invalid_with_lastModifiedDateTime_attribute_value_as_blank_throws_Exception() {
        val tz = TimeZone.getTimeZone("UTC")
        val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ") // Quoted "Z" to indicate UTC, no timezone offset
        df.timeZone = tz
        println(df.format(Date()))
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
    fun null_is_returned_from_json_where_id_is_not_an_object() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":\"test_value\"}}")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson shouldBe null
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

        key shouldBe ByteArray(0)
    }
}
