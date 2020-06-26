package app.services

import com.amazonaws.services.s3.model.S3ObjectInputStream

interface S3Service {
    fun objectInputStream(bucket: String, key: String): S3ObjectInputStream
}
