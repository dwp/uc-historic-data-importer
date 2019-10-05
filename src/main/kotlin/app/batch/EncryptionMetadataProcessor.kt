package app.batch

import app.domain.EncryptedStream
import app.domain.EncryptionMetadata
import app.domain.InputStreamPair
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.batch.item.ItemProcessor
import org.springframework.stereotype.Component

@Component
class EncryptionMetadataProcessor : ItemProcessor<InputStreamPair, EncryptedStream> {

    override fun process(item: InputStreamPair) =
            EncryptedStream(item.dataInputStream, encryptionMetadata(item))

    private fun encryptionMetadata(item: InputStreamPair) =
            ObjectMapper().readValue(item.metadataInputStream,
                                        EncryptionMetadata::class.java)
}
