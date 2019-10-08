package app.batch

import app.domain.EncryptionMetadata
import app.domain.InputStreamPair
import app.exceptions.MetadataException
import org.junit.Test

import org.junit.Assert.*
import java.io.ByteArrayInputStream

class EncryptionMetadataProcessorTest {

    @Test
    fun processOk() {
        val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
        val encryptionKeyId = "encryptionKeyId"
        val plaintextDatakey = "plaintextDatakey"
        val encryptedEncryptionKey = "encryptedEncryptionKey"
        val iv = "iv"
        val validJson = """{
            "encryptionKeyId": "$encryptionKeyId",
            "plaintextDatakey": "$plaintextDatakey",   
            "encryptedEncryptionKey": "$encryptedEncryptionKey",   
            "iv": "$iv"   
        }""".trimIndent().toByteArray()

        val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(validJson), "S3_KEY")
        val result = EncryptionMetadataProcessor().process(pair)
        val expected = EncryptionMetadata(encryptionKeyId, plaintextDatakey, encryptedEncryptionKey, iv)
        assertEquals(expected, result.encryptionMetadata)
    }

    @Test(expected = MetadataException::class)
    fun testMalformedNotOk() {
        val s3key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val iv = "iv"
            val malformedJson = """{
            "encryptionKeyId": "$encryptionKeyId",
            "plaintextDatakey": "$plaintextDatakey",   
            "encryptedEncryptionKey": "$encryptedEncryptionKey",   
            "iv": "$iv"""".trimIndent().toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(malformedJson), "S3_KEY")
            EncryptionMetadataProcessor().process(pair)
        }
        catch (e: MetadataException) {
            val message = "Failed to parse encryption metadata for '$s3key'."
            assertEquals(message, e.message)
            throw e;
        }
    }

    @Test(expected = MetadataException::class)
    fun testExtraFieldsNotOk() {
        val s3Key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val iv = "iv"
            val invalidJson = """{
                "unexpectedKey": "unexpectedValue", 
                "encryptionKeyId": "$encryptionKeyId",
                "plaintextDatakey": "$plaintextDatakey",   
                "encryptedEncryptionKey": "$encryptedEncryptionKey",   
                "iv": "$iv"   
            }""".trimIndent().replace("\n", " ").toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key)
            val result = EncryptionMetadataProcessor().process(pair)
            val expected = EncryptionMetadata(encryptionKeyId, plaintextDatakey, encryptedEncryptionKey, iv)
            assertEquals(expected, result.encryptionMetadata)
        } catch (e: MetadataException) {
            val expected = "Failed to parse encryption metadata for '$s3Key'."
            assertEquals(expected, e.message)
            throw e
        }
    }

    @Test(expected = MetadataException::class)
    fun testMissingEncryptionKeyNotOk() {
        val s3Key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val iv = "iv"
            val invalidJson = """{
                "plaintextDatakey": "$plaintextDatakey",   
                "encryptedEncryptionKey": "$encryptedEncryptionKey",   
                "iv": "$iv"   
            }""".trimIndent().replace("\n", " ").toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key)
            EncryptionMetadataProcessor().process(pair)
        } catch (e: MetadataException) {
            val expected = "Failed to process encryption metadata for '$s3Key': 'encryptionKeyId' encryption metadata field must not be blank."
            assertEquals(expected, e.message)
            throw e
        }
    }

    @Test(expected = MetadataException::class)
    fun testMissingIvNotOk() {
        val s3Key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val iv = "iv"
            val invalidJson = """{
                "encryptionKeyId": "$encryptionKeyId",
                "plaintextDatakey": "$plaintextDatakey",   
                "encryptedEncryptionKey": "$encryptedEncryptionKey"   
            }""".trimIndent().replace("\n", " ").toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key)
            EncryptionMetadataProcessor().process(pair)
        } catch (e: MetadataException) {
            val expected = "Failed to process encryption metadata for '$s3Key': 'iv' encryption metadata field must not be blank."
            assertEquals(expected, e.message)
            throw e
        }
    }

    @Test(expected = MetadataException::class)
    fun testMissingPlaintextDatakeyNotOk() {
        val s3Key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val iv = "iv"
            val invalidJson = """{
                "encryptionKeyId": "$encryptionKeyId",
                "encryptedEncryptionKey": "$encryptedEncryptionKey",
                "iv": "$iv"  
            }""".trimIndent().replace("\n", " ").toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key)
            EncryptionMetadataProcessor().process(pair)
        } catch (e: MetadataException) {
            val expected = "Failed to process encryption metadata for '$s3Key': 'plaintextDatakey' encryption metadata field must not be blank."
            assertEquals(expected, e.message)
            throw e
        }
    }

    @Test(expected = MetadataException::class)
    fun testMissingEncryptedEncryptionKeyDatakeyNotOk() {
        val s3Key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val iv = "iv"
            val invalidJson = """{
                "plaintextDatakey": "$plaintextDatakey",
                "encryptionKeyId": "$encryptionKeyId",
                "iv": "$iv"  
            }""".trimIndent().replace("\n", " ").toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key)
            EncryptionMetadataProcessor().process(pair)
        } catch (e: MetadataException) {
            val expected = "Failed to process encryption metadata for '$s3Key': 'encryptedEncryptionKey' encryption metadata field must not be blank."
            assertEquals(expected, e.message)
            throw e
        }
    }
}