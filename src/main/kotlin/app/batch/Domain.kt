package app.batch

import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.model.S3ObjectSummary

data class KeyPair(val dataKey: String?, val metadataKey: String?)
data class EncryptedStream(val dataInputStream: S3ObjectInputStream?, val metadataInputStream: S3ObjectInputStream?)
data class S3ObjectSummaryPair(val data: S3ObjectSummary?, val metadata: S3ObjectSummary?)

//TODO Can  this nullable be avoided

