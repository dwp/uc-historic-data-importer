package app.configuration

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.sift.Discriminator


class ThreadNameBasedDiscriminator : Discriminator<ILoggingEvent> {
    private val KEY = "threadName"

    private var started = false

    override fun getDiscriminatingValue(iLoggingEvent: ILoggingEvent?): String? {
        return Thread.currentThread().name
    }

    override fun getKey(): String? {
        return KEY
    }

    override fun start() {
        started = true
    }

    override fun stop() {
        started = false
    }

    override fun isStarted(): Boolean {
        return started
    }
}
