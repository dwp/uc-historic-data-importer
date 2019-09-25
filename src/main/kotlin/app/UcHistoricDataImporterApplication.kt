package app

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration
import org.springframework.boot.runApplication
import kotlin.system.exitProcess

@SpringBootApplication(exclude = [DataSourceAutoConfiguration::class])
class UcHistoricDataImporterApplication: CommandLineRunner {
	override fun run(vararg args: String?) {

	}

}

fun main(args: Array<String>) {
	exitProcess(SpringApplication.exit(runApplication<UcHistoricDataImporterApplication>(*args)))
}
