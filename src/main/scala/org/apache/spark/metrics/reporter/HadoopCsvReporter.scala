package org.apache.spark.metrics.reporter

import com.codahale.metrics._
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.writer.HadoopFileWriter

import java.nio.file.Paths
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit

import java.io.IOException
import java.util.Locale

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class HadoopCsvReporter(rootDir: String,
                        registry: MetricRegistry,
                        locale: Locale,
                        separator: String,
                        rateUnit: TimeUnit,
                        durationUnit: TimeUnit,
                        clock: Clock,
                        filter: MetricFilter,
                        fileWriter: HadoopFileWriter,
                        appName: Option[String])
                       (implicit logger: SparkScopeLogger)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter) {
    logger.info("Using HadoopCsvReporter", this.getClass)

    override protected[reporter] def report(appId: String, instance: String, metrics: DataTable, timestamp: Long): Unit = {
        logger.debug("\n" + metrics.toString, this.getClass)
        val row: String = metrics.toCsvNoHeader(separator)

        val appDir = Paths.get(rootDir, appName.getOrElse(""), appId).toString
        val csvFilePath = Paths.get(appDir, s"${instance}.csv").toString


        try {
            logger.debug(s"Writing to ${csvFilePath}", this.getClass)
            if (!fileWriter.exists(csvFilePath)) {
                fileWriter.makeDir(appDir)
                fileWriter.write(csvFilePath, metrics.header + "\n")
            }

            logger.debug(s"Writing row: ${row}", this.getClass)
            fileWriter.append(csvFilePath, row + "\n")
        } catch {
            case e: IOException => logger.warn(s"IOException while writing ${instance} to ${csvFilePath}. ${e}", this.getClass)
        }
    }
}
