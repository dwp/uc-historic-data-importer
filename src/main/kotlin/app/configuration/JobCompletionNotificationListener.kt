package app.configuration

import app.batch.HbaseClient
import app.utils.logging.JsonLoggerWrapper
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.stereotype.Component

@Component
class JobCompletionNotificationListener(private val hbaseClient: HbaseClient) : JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        hbaseClient.close()
        logger.info("Finished, status: '${jobExecution.status}'.")
    }

    companion object {
        private val logger: JsonLoggerWrapper = JsonLoggerWrapper.getLogger(JobCompletionNotificationListener::class.toString())
    }
}
