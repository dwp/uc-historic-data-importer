package app.batch

import com.amazonaws.services.s3.model.S3ObjectSummary
import java.io.InputStream

data class KeyPair(val dataKey: String?, val metadataKey: String?)
data class EncryptedStream(val dataInputStream: InputStream?, val metadataInputStream: InputStream?)
data class DecryptedStream(val inputStream: InputStream, val fileName: String)
data class S3ObjectSummaryPair(val data: S3ObjectSummary?, val metadata: S3ObjectSummary?)

data class DataKeyResult(val dataKeyEncryptionKeyId: String, val plaintextDataKey: String, val ciphertextDataKey: String)
data class EncryptionResult(val initialisationVector: String, val encrypted: String)

