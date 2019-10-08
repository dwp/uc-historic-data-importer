package app.services

import app.batch.EncryptionResult


interface CipherService {
    fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult
}