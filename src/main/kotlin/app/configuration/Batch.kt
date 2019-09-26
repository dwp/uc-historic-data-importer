package app.configuration

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile

@Configuration
@EnableBatchProcessing
@Profile("batchRun")
class Batch {

//    @Bean
//    fun importUserJob(step: Step) =
//            jobBuilderFactory.get("snapshotSenderJob")
//                    .incrementer(RunIdIncrementer())
//                    .flow(step)
//                    .end()
//                    .build()

//    @Bean
//    fun step() =
//            stepBuilderFactory.get("step")
//                    .chunk<Object, Object>(10)
//                    .reader(itemReader)
//                    .processor(itemProcessor())
//                    .writer(itemWriter)
//                    .build()


    @Autowired
    lateinit var jobBuilderFactory: JobBuilderFactory

    @Autowired
    lateinit var stepBuilderFactory: StepBuilderFactory
}