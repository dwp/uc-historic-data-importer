package app.configuration

import app.batch.EncryptedStream
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
        jobBuilderFactory.get("snapshotSenderJob")
            .incrementer(RunIdIncrementer())
            .flow(step)
            .end()
            .build()

    @Bean
    fun step() =
        stepBuilderFactory.get("step")
            .chunk<EncryptedStream, EncryptedStream>(10)
            .reader(itemReader)
            .processor(itemProcessor())
            .writer(itemWriter)
            .taskExecutor(taskExecutor())
            .build()

    @Bean
    fun taskExecutor() = SimpleAsyncTaskExecutor("uc-historic-data-importer").apply {
        concurrencyLimit = Integer.parseInt(threadCount)
    }

    fun itemProcessor(): ItemProcessor<EncryptedStream, EncryptedStream> =
        CompositeItemProcessor<EncryptedStream, EncryptedStream>().apply {
            setDelegates(listOf(decryptionProcessor, encryptionProcessor))
        }

    @Autowired
    lateinit var itemReader: ItemReader<EncryptedStream>

    @Autowired
    lateinit var encryptionProcessor: ItemProcessor<EncryptedStream, EncryptedStream>

    @Autowired
    lateinit var decryptionProcessor: ItemProcessor<EncryptedStream, EncryptedStream>

    @Autowired
    lateinit var itemWriter: ItemWriter<EncryptedStream>

    @Autowired
    lateinit var jobBuilderFactory: JobBuilderFactory

    @Autowired
    lateinit var stepBuilderFactory: StepBuilderFactory

    @Value("\${thread.count:10}")
    lateinit var threadCount: String;
}
