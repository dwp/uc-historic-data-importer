package app.services

import app.domain.EncryptionResult

interface CipherService {
    fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult
}