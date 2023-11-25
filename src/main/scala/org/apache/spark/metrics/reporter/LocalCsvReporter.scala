package org.apache.spark.metrics.reporter

import com.codahale.metrics._
import com.ucesys.sparkscope.common.Metric
import com.ucesys.sparkscope.io.file.LocalFileWriter
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.{File, FileOutputStream, IOException, OutputStreamWriter, PrintWriter}
import java.util.Locale
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Paths

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class LocalCsvReporter(directory: String,
                       registry: MetricRegistry,
                       locale: Locale,
                       separator: String,
                       rateUnit: TimeUnit,
                       durationUnit: TimeUnit,
                       clock: Clock,
                       filter: MetricFilter,
                       executor: ScheduledExecutorService,
                       shutdownExecutorOnStop: Boolean,
                       fileWriter: LocalFileWriter)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop) {

    /**
     * A builder for {@link LocalCsvReporter} instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */

    private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)
    LOGGER.info("Using LocalCsvReporter")

    override protected[reporter] def report(metric: Metric, header: String, row: String, timestamp: Long): Unit = {
        LOGGER.debug(s"name: ${metric.fullName}, header: ${header}, row: ${row}")

        val csvFilePath = Paths.get(directory, s"${metric.fullName}.csv").toString

        try {
            LOGGER.debug(s"Writing to ${csvFilePath}")
            if (!fileWriter.exists(csvFilePath)) {
                fileWriter.write(csvFilePath, s"t${separator}${header}\n");
            }

            fileWriter.append(csvFilePath, row);
        } catch {
            case e: IOException => LOGGER.warn(s"Error writing ${metric.fullName} to local dir ${directory}. ${e}")
        }
    }
}
