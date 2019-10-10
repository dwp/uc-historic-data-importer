package app.exceptions

class DataKeyDecryptionException(message: String) : Exception(message)

class DataKeyServiceUnavailableException(message: String) : Exception(message)

class WriterException(message: String) : Exception(message)

class MetadataException : Exception {
    constructor(message: String) : super(message)
    constructor(message: String, e: Throwable) : super(message, e)
}

class DecryptionException(message: String, e: Throwable) : Exception(message, e)