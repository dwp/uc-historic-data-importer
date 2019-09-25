package app

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class UcHistoricDataImporterApplication

fun main(args: Array<String>) {
	runApplication<UcHistoricDataImporterApplication>(*args)
}
