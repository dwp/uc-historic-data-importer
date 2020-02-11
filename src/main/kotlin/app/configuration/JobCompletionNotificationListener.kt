package app.configuration

import app.batch.HbaseClient
import app.utils.logging.logInfo
import org.slf4j.LoggerFactory
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.listener.JobExecutionListenerSupport
import org.springframework.stereotype.Component

@Component
class JobCompletionNotificationListener(private val hbaseClient: HbaseClient) : JobExecutionListenerSupport() {

    override fun afterJob(jobExecution: JobExecution) {
        hbaseClient.close()
        logInfo(logger, "Finished, status: '${jobExecution.status}'.")
    }

    companion object {
        private val logger = LoggerFactory.getLogger(JobCompletionNotificationListener::class.java)
    }
}
