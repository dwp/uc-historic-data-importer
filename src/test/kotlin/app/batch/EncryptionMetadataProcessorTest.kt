package app.batch

import app.domain.EncryptionMetadata
import app.domain.InputStreamPair
import app.exceptions.MetadataException
import org.junit.Assert.assertEquals
import org.junit.Test
import java.io.ByteArrayInputStream

class EncryptionMetadataProcessorTest {

    @Test
    fun testWellFormedEncryptionMetadataOk() {
        val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
        val encryptionKeyId = "encryptionKeyId"
        val plaintextDatakey = "plaintextDatakey"
        val encryptedEncryptionKey = "encryptedEncryptionKey"
        val keyEncryptionCipher = "keyEncryptionCipher"
        val iv = "iv"
        val validJson = """{
            "keyEncryptionKeyId": "$encryptionKeyId",
            "plaintextDatakey": "$plaintextDatakey",   
            "encryptedEncryptionKey": "$encryptedEncryptionKey",   
            "initialisationVector": "$iv",
            "encryptionCipher": "encryptionCipher",
            "keyEncryptionCipher": "$keyEncryptionCipher"
        }""".trimIndent().toByteArray()

        val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(validJson), "S3_KEY", 100L)
        val result = EncryptionMetadataProcessor().process(pair)
        val expected = EncryptionMetadata(encryptionKeyId, plaintextDatakey, encryptedEncryptionKey, iv,
            encryptionCipher = "encryptionCipher",
            keyEncryptionCipher = "keyEncryptionCipher")
        assertEquals(expected, result.encryptionMetadata)
    }

    @Test(expected = MetadataException::class)
    fun testMalformedEncryptionMetadataNotOk() {
        val s3key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val iv = "iv"
            val malformedJson = """{
            "keyEncryptionKeyId": "$encryptionKeyId",
            "plaintextDatakey": "$plaintextDatakey",   
            "encryptedEncryptionKey": "$encryptedEncryptionKey",   
            "initialisationVector": "$iv"""".trimIndent().toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(malformedJson), "S3_KEY", 100L)
            EncryptionMetadataProcessor().process(pair)
        }
        catch (e: MetadataException) {
            val message = "Failed to parse encryption metadata for '$s3key'."
            assertEquals(message, e.message)
            throw e
        }
    }

    @Test
    fun testEncryptionMetadataWithExtraFieldsOk() {
        val s3Key = "S3_KEY"
        val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
        val encryptionKeyId = "encryptionKeyId"
        val plaintextDatakey = "plaintextDatakey"
        val encryptedEncryptionKey = "encryptedEncryptionKey"
        val iv = "iv"
        val invalidJson = """{
            "unexpectedKey": "unexpectedValue", 
            "keyEncryptionKeyId": "$encryptionKeyId",
            "plaintextDatakey": "$plaintextDatakey",   
            "encryptedEncryptionKey": "$encryptedEncryptionKey",   
            "initialisationVector": "$iv"   
        }""".trimIndent().replace("\n", " ").toByteArray()

        val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key, 100L)
        val result = EncryptionMetadataProcessor().process(pair)
        val expected = EncryptionMetadata(encryptionKeyId, plaintextDatakey, encryptedEncryptionKey, iv, "")
        assertEquals(expected, result.encryptionMetadata)
    }

    @Test(expected = MetadataException::class)
    fun testEncryptionMetadataWithMissingEncryptionKeyNotOk() {
        val s3Key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val iv = "iv"
            val invalidJson = """{
                "plaintextDatakey": "$plaintextDatakey",   
                "encryptedEncryptionKey": "$encryptedEncryptionKey",   
                "initialisationVector": "$iv"   
            }""".trimIndent().replace("\n", " ").toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key, 100L)
            EncryptionMetadataProcessor().process(pair)
        }
        catch (e: MetadataException) {
            val expected = "Failed to process encryption metadata for '$s3Key': 'keyEncryptionKeyId' encryption metadata field must not be blank."
            assertEquals(expected, e.message)
            throw e
        }
    }

    @Test(expected = MetadataException::class)
    fun testEncryptionMetadataWithMissingIvNotOk() {
        val s3Key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val encryptedEncryptionKey = "encryptedEncryptionKey"
            val invalidJson = """{
                "keyEncryptionKeyId": "$encryptionKeyId",
                "plaintextDatakey": "$plaintextDatakey",   
                "encryptedEncryptionKey": "$encryptedEncryptionKey"   
            }""".trimIndent().replace("\n", " ").toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key, 100L)
            EncryptionMetadataProcessor().process(pair)
        }
        catch (e: MetadataException) {
            val expected = "Failed to process encryption metadata for '$s3Key': 'initialisationVector' encryption metadata field must not be blank."
            assertEquals(expected, e.message)
            throw e
        }
    }


    @Test(expected = MetadataException::class)
    fun testEncryptionMetadataWithMissingEncryptedEncryptionKeyDatakeyNotOk() {
        val s3Key = "S3_KEY"
        try {
            val dataInputStream = ByteArrayInputStream("DATA INPUT STREAM".toByteArray())
            val encryptionKeyId = "encryptionKeyId"
            val plaintextDatakey = "plaintextDatakey"
            val iv = "iv"
            val invalidJson = """{
                "plaintextDatakey": "$plaintextDatakey",
                "keyEncryptionKeyId": "$encryptionKeyId",
                "initialisationVector": "$iv"  
            }""".trimIndent().replace("\n", " ").toByteArray()

            val pair = InputStreamPair(dataInputStream, ByteArrayInputStream(invalidJson), s3Key, 100L)
            EncryptionMetadataProcessor().process(pair)
        }
        catch (e: MetadataException) {
            val expected = "Failed to process encryption metadata for '$s3Key': 'encryptedEncryptionKey' encryption metadata field must not be blank."
            assertEquals(expected, e.message)
            throw e
        }
    }
}
