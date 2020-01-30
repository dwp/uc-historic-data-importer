package app.services

import app.domain.EncryptionResult
import java.io.InputStream
import java.security.Key

interface CipherService {
    fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult
    fun decompressingDecryptingStream(inputStream: InputStream, key: Key, iv: String): InputStream
}
