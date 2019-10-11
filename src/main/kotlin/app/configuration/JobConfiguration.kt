package app.configuration

import app.domain.DecompressedStream
import app.domain.DecryptedStream
import app.domain.EncryptedStream
import app.domain.InputStreamPair
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.launch.support.RunIdIncrementer
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.support.CompositeItemProcessor
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import org.springframework.core.task.SimpleAsyncTaskExecutor

@Configuration
@EnableBatchProcessing
@Profile("batchRun")
class JobConfiguration {

    @Bean
    fun importUserJob(step: Step) =
        jobBuilderFactory.get("ucHistoricDataImporterJob")
            .incrementer(RunIdIncrementer())
            .flow(step)
            .end()
            .build()

    @Bean
    fun step() =
        stepBuilderFactory.get("step")
            .chunk<InputStreamPair, DecompressedStream>(1)
            .reader(itemReader)
            .processor(itemProcessor())
            .writer(itemWriter)
            .taskExecutor(taskExecutor())
            .build()

    @Bean
    fun taskExecutor() = SimpleAsyncTaskExecutor("uc-historic-data-importer").apply {
        concurrencyLimit = Integer.parseInt(threadCount)
    }

    fun itemProcessor(): ItemProcessor<InputStreamPair, DecompressedStream> =
        CompositeItemProcessor<InputStreamPair, DecompressedStream>().apply {
            setDelegates(listOf(encryptionMetadataProcessor,
                datakeyProcessor,
                decryptionProcessor,
                decompressionProcessor))
        }

    @Autowired
    lateinit var itemReader: ItemReader<InputStreamPair>

    @Autowired
    lateinit var encryptionMetadataProcessor: ItemProcessor<InputStreamPair, EncryptedStream>

    @Autowired
    lateinit var datakeyProcessor: ItemProcessor<EncryptedStream, EncryptedStream>

    @Autowired
    lateinit var decompressionProcessor: ItemProcessor<DecryptedStream, DecompressedStream>

    @Autowired
    lateinit var decryptionProcessor: ItemProcessor<EncryptedStream, DecryptedStream>

    @Autowired
    lateinit var itemWriter: ItemWriter<DecompressedStream>

    @Autowired
    lateinit var jobBuilderFactory: JobBuilderFactory

    @Autowired
    lateinit var stepBuilderFactory: StepBuilderFactory

    @Value("\${thread.count:1}")
    lateinit var threadCount: String
}
