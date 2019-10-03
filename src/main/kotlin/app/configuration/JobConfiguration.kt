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
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

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
                    .chunk<EncryptedStream, String>(10)
                    .reader(itemReader)
                    .processor(itemProcessor())
                    .writer(itemWriter)
                    .build()

    fun itemProcessor(): ItemProcessor<EncryptedStream, String> =
            CompositeItemProcessor<EncryptedStream, String>().apply {
                setDelegates(listOf(decryptionProcessor, encryptionProcessor))
            }

    @Autowired
    lateinit var itemReader: ItemReader<EncryptedStream>

    @Autowired
    lateinit var encryptionProcessor: ItemProcessor<String, String>

    @Autowired
    lateinit var decryptionProcessor: ItemProcessor<String, String>

    @Autowired
    lateinit var itemWriter: ItemWriter<String>

    @Autowired
    lateinit var jobBuilderFactory: JobBuilderFactory

    @Autowired
    lateinit var stepBuilderFactory: StepBuilderFactory
}