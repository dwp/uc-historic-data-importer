package app.exceptions

class DataKeyDecryptionException(message: String): Exception(message)

class DataKeyServiceUnavailableException(message: String): Exception(message)

class WriterException(message: String): Exception(message)

class MetadataException(message: String, e: Throwable): Exception(message, e)

class DecryptionException(message: String, e: Throwable): Exception(message, e)