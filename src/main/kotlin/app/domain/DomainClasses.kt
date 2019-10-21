package app.domain

import com.amazonaws.services.s3.model.S3ObjectSummary
import java.io.InputStream

data class KeyPair(val dataKey: String?, val metadataKey: String?)
data class InputStreamPair(val dataInputStream: InputStream, val metadataInputStream: InputStream,
                           val s3key: String, val objectSize: Long)
data class S3ObjectSummaryPair(val data: S3ObjectSummary?, val metadata: S3ObjectSummary?)

data class EncryptionMetadata(val encryptionKeyId: String = "",
                              var plaintextDatakey: String = "",
                              val encryptedEncryptionKey: String = "",
                              val iv: String = "")

data class EncryptedStream(val dataInputStream: InputStream, val encryptionMetadata: EncryptionMetadata, val s3key: String)
data class DataKeyResult(val dataKeyEncryptionKeyId: String, val plaintextDataKey: String, val ciphertextDataKey: String)
data class DecryptedStream(val inputStream: InputStream, val fileName: String)
data class DecompressedStream(val inputStream: InputStream, val fileName: String)
data class EncryptionResult(val initialisationVector: String, val encrypted: String)
