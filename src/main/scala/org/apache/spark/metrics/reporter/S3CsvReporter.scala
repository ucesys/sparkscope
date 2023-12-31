package org.apache.spark.metrics.reporter;

import com.codahale.metrics.Clock
import com.codahale.metrics.MetricFilter
import com.codahale.metrics.MetricRegistry
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.file.S3FileWriter
import com.ucesys.sparkscope.io.metrics.S3Location

import java.nio.file.Paths
import java.util.Locale
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class S3CsvReporter(directory: String,
                    registry: MetricRegistry,
                    locale: Locale,
                    separator: String,
                    rateUnit: TimeUnit,
                    durationUnit: TimeUnit,
                    clock: Clock,
                    filter: MetricFilter,
                    executor: ScheduledExecutorService,
                    shutdownExecutorOnStop: Boolean,
                    fileWriter: S3FileWriter)
                   (implicit logger: SparkScopeLogger)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop) {
    val s3Location: S3Location = S3Location(directory)
    var isInit: Boolean = false;

    logger.info("Using S3CsvReporter", this.getClass)

    override protected[reporter] def report(appId: String, instance: String, metrics: DataTable, timestamp: Long): Unit = {
        logger.debug("\n" + metrics.toString, this.getClass)

        val appPath: String = Paths.get(this.s3Location.path,".tmp", appId).toString
        val metricPath: String = Paths.get(appPath, instance, instance + "." + timestamp + ".csv").toString;
        val inProgressPath: String = Paths.get(appPath,"IN_PROGRESS").toString;

        val metricS3Location: S3Location = this.s3Location.copy(path = metricPath)
        val inProgressS3Location: S3Location = this.s3Location.copy(path = inProgressPath)

        if(!this.isInit) {
            if (!fileWriter.exists(inProgressS3Location.getUrl)) {
                fileWriter.write(inProgressS3Location.getUrl, "")
            }
            this.isInit = true;
        }

        logger.debug(s"Writing csv: ${instance} metrics to ${inProgressS3Location.getUrl}", this.getClass)
        if (fileWriter.exists(inProgressS3Location.getUrl)) {
            fileWriter.write(metricS3Location.getUrl, metrics.toCsv(separator))
        }
    }
}
