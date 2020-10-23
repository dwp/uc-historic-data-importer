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
import org.junit.Assert.assertEquals


class MessageUtilsTest {

    val messageUtils = MessageUtils()

    @Test
    fun validInputConvertsToJson() {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = messageUtils.parseJson(jsonString)

        json should beInstanceOf<JsonObject>()
        json.string("testOne") shouldBe "test1"
        json.int("testTwo") shouldBe 2
    }

    @Test
    fun validNestedInputConvertsToJson() {
        val jsonString = "{\"testOne\":{\"testTwo\":2}}"
        val json: JsonObject = messageUtils.parseJson(jsonString)
        val jsonTwo: JsonObject = json.obj("testOne") as JsonObject

        json should beInstanceOf<JsonObject>()
        jsonTwo.int("testTwo") shouldBe 2
    }

    @Test
    fun invalidNestedInputThrowsException() {
        val jsonString = "{\"testOne\":"

        shouldThrow<KlaxonException> {
            messageUtils.parseJson(jsonString)
        }
    }

    @Test
    fun sortsJsonByKeyName() {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonObjectUnsorted: JsonObject = messageUtils.parseJson(jsonStringUnsorted)
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = messageUtils.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun sortsJsonByKeyNameCaseSensitively() {
        val jsonStringUnsorted = "{\"testb\":true, \"testA\":\"test1\", \"testC\":2}"
        val jsonObjectUnsorted: JsonObject = messageUtils.parseJson(jsonStringUnsorted)
        val jsonStringSorted = "{\"testA\":\"test1\",\"testC\":2,\"testb\":true}"

        val sortedJson = messageUtils.sortJsonByKey(jsonObjectUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun checksumsAreDifferentWithDifferentInputs() {
        val jsonStringOne = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val jsonStringTwo = "{\"testOne\":\"test2\", \"testTwo\":2}"
        val checksum = messageUtils.generateFourByteChecksum(jsonStringOne)
        val checksumTwo = messageUtils.generateFourByteChecksum(jsonStringTwo)

        checksum shouldNotBe checksumTwo
    }

    @Test
    fun canGenerateConsistentChecksumsFromJson() {
        val jsonString = "{\"testOne\":\"test1\", \"testTwo\":2}"
        val json: JsonObject = messageUtils.parseJson(jsonString)
        val checksumOne = messageUtils.generateFourByteChecksum(json.toString())
        val checksumTwo = messageUtils.generateFourByteChecksum(json.toString())

        checksumOne shouldBe checksumTwo
    }

    @Test
    fun generatedChecksumsAreFourBytes() {
        assertAll { input: String ->
            val checksum = messageUtils.generateFourByteChecksum(input)
            checksum.size shouldBe 4
        }
    }

    @Test
    fun validTimestampFormatInTheMessageGetsParsedAsLongCorrectly() {
        val timeStampAsLong = messageUtils.getTimestampAsLong("2018-12-14T15:01:02.000Z")
        timeStampAsLong shouldBe 1544799662000
    }

    @Test
    fun invalidTimestampFormatInTheMessageThrowsException() {
        shouldThrow<ParseException> {
            messageUtils.getTimestampAsLong("2018-12-14")
        }
    }

    @Test
    fun validDatesWithoutTimezoneParseCorrectly() {
        1544799662000L shouldBe messageUtils.getTimestampAsLong("2018-12-14T15:01:02.000Z")
    }

    @Test
    fun validDatesWithTimezoneParseCorrectly() {
        1544799662000L shouldBe messageUtils.getTimestampAsLong("2018-12-14T15:01:02.000+0000")
    }

    @Test
    fun invalidJsonWithLastModifiedDateTimeAttributeValueAsEmptyThrowsException() {
        shouldThrow<ParseException> {
            messageUtils.getTimestampAsLong("")
        }
    }

    @Test
    fun invalidJsonWithLastModifiedDateTimeAttributeValueAsBlankThrowsException() {
        shouldThrow<ParseException> {
            messageUtils.getTimestampAsLong("   ")
        }
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputs() {

        val json: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")

        val keyOne: ByteArray = messageUtils.generateKey(json)
        val keyTwo: ByteArray = messageUtils.generateKey(json)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generatedKeysAreDifferentForDifferentInputs() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":3}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfOrder() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testTwo\":2, \"testOne\":\"test1\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfWhitespace() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{    \"testOne\":              \"test1\",            \"testTwo\":  2}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generatedKeysAreConsistentForIdenticalInputsRegardlessOfOrderAndWhitespace() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{    \"testTwo\":              2,            \"testOne\":  \"test1\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe true
    }

    @Test
    fun generatedKeysWilVaryGivenValueWithDifferentWhitespace() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test 1\", \"testTwo\":2}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysWilVaryGivenValuesThatAreStringAndIntInEachInput() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":\"2\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysWilVaryGivenValuesThatAreStringAndFloatInEachInput() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":2.0}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":\"2.0\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysWilVaryGivenValuesThatAreStringAndBooleanInEachInput() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":false}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":\"false\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun generatedKeysWilVaryGivenValuesThatAreStringAndNullInEachInput() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":null}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"testOne\":\"test1\", \"testTwo\":\"null\"}")

        val keyOne: ByteArray = messageUtils.generateKey(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKey(jsonTwo)

        keyOne.contentEquals(keyTwo) shouldBe false
    }

    @Test
    fun idIsReturnedFromValidJson() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":{\"test_key\":\"test_value\"}}}")
        val idString = "{\"test_key\":\"test_value\"}"

        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson?.toJsonString() shouldBe idString
    }

    @Test
    fun nullIsReturnedFromValidJsonWhereMessageDoesNotExist() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"test_object\":{\"_id\":{\"test_key\":\"test_value\"}}}")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson shouldBe null
    }

    @Test
    fun nullIsReturnedFromValidJsonWhereMessageIsNotAnObject() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":\"test_value\"}")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson shouldBe null
    }

    @Test
    fun nullIsReturnedFromValidJsonWhereIdIsMissing() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"test_object\":{\"test_key\":\"test_value\"}}}")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)

        idJson shouldBe null
    }


    @Test
    fun alteredIdIsReturnedFromValidJsonWhereIdIsNotAnObject() {

        val jsonOne: JsonObject = messageUtils.parseJson("""{"message":{"_id":"test_value"}}""")
        val idJson: JsonObject? = messageUtils.getId(jsonOne)
        val expected = JsonObject(mutableMapOf("id" to "test_value"))
        idJson shouldBe expected
    }

    @Test
    fun generatedKeyIsConsistentForIdenticalRecordBodyIndependantOfKeyOrderAndWhitespace() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":{\"test_key_a\":\"test_value_a\",\"test_key_b\"    :\"test_value_b\"}}}")
        val jsonTwo: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":{\"test_key_b\":     \"test_value_b\",\"test_key_a\":\"test_value_a\"}}}")

        val keyOne: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)
        val keyTwo: ByteArray = messageUtils.generateKeyFromRecordBody(jsonTwo)

        keyOne shouldNotBe ByteArray(0)
        keyOne shouldBe keyTwo
    }


    @Test
    fun emptyIsReturnedFromRecordBodyKeyGenerationWhereMessageDoesNotExist() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"test_object\":{\"_id\":{\"test_key\":\"test_value\"}}}")
        val key: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    @Test
    fun emptyIsReturnedFromRecordBodyKeyGenerationWhereMessageIsNotAnObject() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":\"test_value\"}")
        val key: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    @Test
    fun emptyIsReturnedFromRecordBodyKeyGenerationWhereIdIsMissing() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"test_object\":{\"test_key\":\"test_value\"}}}")
        val key: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)

        key shouldBe ByteArray(0)
    }

    @Test
    fun emptyIsReturnedFromRecordBodyKeyGenerationWhereIdIsNotAnObject() {

        val jsonOne: JsonObject = messageUtils.parseJson("{\"message\":{\"_id\":\"test_value\"}}")
        val key: ByteArray = messageUtils.generateKeyFromRecordBody(jsonOne)
        val noChecksum = String(key).substring(3)
        val expected = """{"id":"test_value"}"""
        noChecksum shouldBe expected
    }

    @Test
    fun typeIsReturnedFromValidInput() {
        val jsonString = """{
            "message": {
               "@type": "MONGO_IMPORT_TWO",
            }
        }"""

        val json: JsonObject = messageUtils.parseJson(jsonString)
        messageUtils.getType(json) shouldBe "MONGO_IMPORT_TWO"
    }

    @Test
    fun defaultTypeIsReturnedFromInvalidInput() {
        val jsonString = """{
            "message": {
               "notype": "HDI",
            }
        }"""

        val json: JsonObject = messageUtils.parseJson(jsonString)
        messageUtils.getType(json) shouldBe "MONGO_IMPORT"
    }

    @Test
    fun shouldSortJsonByKeyName() {
        val jsonStringUnsorted = "{\"testA\":\"test1\", \"testC\":2, \"testB\":true}"
        val jsonStringSorted = "{\"testA\":\"test1\",\"testB\":true,\"testC\":2}"

        val sortedJson = messageUtils.sortJsonStringByKey(jsonStringUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun shouldSortJsonByKeyNameForSingleKey() {
        val jsonStringUnsorted = "{\"\$oid\":\"test1\"}"
        val jsonStringSorted = "{\"\$oid\":\"test1\"}"

        val sortedJson = messageUtils.sortJsonStringByKey(jsonStringUnsorted)

        sortedJson shouldBe jsonStringSorted
    }

    @Test
    fun shouldSortJsonByKeyNameCaseSensitively() {
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
        val actual = messageUtils.getVersion(innerType, lastModifiedDate, removedDate,
                archivedDate)
        assertEquals(lastModifiedDate, actual)
    }

    @Test
    fun testGetVersionUsesLastModifiedWhenMongoInsert() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_INSERT"
        val removedDate = "2000-01-01T00:00:00.000Z"
        val archivedDate = "2005-01-01T00:00:00.000Z"
        val actual = messageUtils.getVersion(innerType, lastModifiedDate, removedDate,
                archivedDate)
        assertEquals(lastModifiedDate, actual)
    }

    @Test
    fun testGetVersionToleratesGarbageWhenNotDelete() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_IMPORT"
        val removedDate = "NON PARSEABLE REMOVED DATE"
        val archivedDate = "NON PARSEABLE ARCHIVED DATE"
        val actual = messageUtils.getVersion(innerType, lastModifiedDate,
                removedDate, archivedDate)
        assertEquals(lastModifiedDate, actual)
    }

    @Test
    fun testGetVersionUsesRemovedDateWhenMongoDelete() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_DELETE"
        val removedDate = "2000-01-01T00:00:00.000Z"
        val archivedDate = "2005-01-01T00:00:00.000Z"
        val actual = messageUtils.getVersion(innerType, lastModifiedDate, removedDate,
                archivedDate)
        assertEquals(removedDate, actual)
    }

    @Test
    fun testGetVersionUsesArchivedDateWhenMongoDeleteButEmptyRemovedDate() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_DELETE"
        val removedDate = ""
        val archivedDate = "2005-01-01T00:00:00.000Z"
        val actual = messageUtils.getVersion(innerType, lastModifiedDate,
                removedDate, archivedDate)
        assertEquals(archivedDate, actual)
    }

    @Test
    fun testGetVersionUsesLastModifiedWhenMongoDeleteButEmptyRemovedDateAndArchivedDate() {
        val lastModifiedDate = "2001-01-01T00:00:00.000Z"
        val innerType = "MONGO_DELETE"
        val removedDate = ""
        val archivedDate = ""
        val actual = messageUtils.getVersion(innerType, lastModifiedDate,
                removedDate, archivedDate)
        assertEquals(lastModifiedDate, actual)
    }
}
