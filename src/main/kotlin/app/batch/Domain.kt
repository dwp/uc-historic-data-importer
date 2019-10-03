package app.batch

import com.amazonaws.services.s3.model.S3ObjectSummary
import java.io.InputStream

data class KeyPair(val dataKey: String?, val metadataKey: String?)
data class EncryptedStream(val dataInputStream: InputStream?, val metadataInputStream: InputStream?)
data class S3ObjectSummaryPair(val data: S3ObjectSummary?, val metadata: S3ObjectSummary?)

