package org.apache.spark.metrics.reporter

import com.codahale.metrics._
import com.ucesys.sparkscope.common.Metric
import com.ucesys.sparkscope.io.file.HadoopFileWriter
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Paths
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import org.apache.hadoop.fs.Path

import java.io.IOException
import java.util.Locale

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class HadoopCsvReporter(directory: String,
                        registry: MetricRegistry,
                        locale: Locale,
                        separator: String,
                        rateUnit: TimeUnit,
                        durationUnit: TimeUnit,
                        clock: Clock,
                        filter: MetricFilter,
                        executor: ScheduledExecutorService,
                        shutdownExecutorOnStop: Boolean,
                        fileWriter: HadoopFileWriter)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop) {
    private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)
    LOGGER.info("Using HadoopCsvReporter")

    override protected[reporter] def report(metric: Metric, header: String, row: String, timestamp: Long): Unit = {
        LOGGER.debug(s"name: ${metric.fullName}, header: ${header}, row: ${row}")
        val path: Path = new Path(Paths.get(directory, s"${metric.fullName}.csv").toString)

        try {
            LOGGER.debug(s"Writing to ${path.toString}")
            if (!fileWriter.exists(path.toString)) {
                fileWriter.write(path.toString, "t" + separator + header + "\n")
            }

            LOGGER.debug(s"Writing row: ${row}")
            fileWriter.append(path.toString, row + "\n")
        } catch {
            case e: IOException => LOGGER.warn(s"IOException while writing ${metric.fullName} to ${directory}. ${e}")
        }
    }
}
