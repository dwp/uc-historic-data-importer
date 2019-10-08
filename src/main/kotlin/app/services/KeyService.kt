package app.services

import app.batch.DataKeyResult
import app.exceptions.DataKeyServiceUnavailableException

interface KeyService {

    @Throws(DataKeyServiceUnavailableException::class)
    fun batchDataKey(): DataKeyResult
}