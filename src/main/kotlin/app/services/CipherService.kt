package app.services

import app.domain.EncryptionResult
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import java.io.InputStream
import java.security.Key
import javax.crypto.Cipher
import javax.crypto.CipherInputStream

interface CipherService {
    fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult
    fun decompressingDecryptingStream(inputStream: InputStream, key: Key, iv: String): InputStream
}
