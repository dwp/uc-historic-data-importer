package app.configuration

import javax.crypto.Cipher

interface CipherInstanceProvider {
    fun cipherInstance(): Cipher
}