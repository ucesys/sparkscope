package org.apache.spark.metrics.reporter;

import com.codahale.metrics.Clock
import com.codahale.metrics.MetricFilter
import com.codahale.metrics.MetricRegistry
import com.ucesys.sparkscope.common.Metric
import com.ucesys.sparkscope.io.file.S3FileWriter
import com.ucesys.sparkscope.io.metrics.S3Location
import org.slf4j.Logger
import org.slf4j.LoggerFactory

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
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop) {
    val s3Location: S3Location = S3Location(directory)
    val LOGGER: Logger = LoggerFactory.getLogger(this.getClass);
    var isInit: Boolean = false;

    override protected[reporter] def report(metric: Metric, header: String, row: String, timestamp: Long): Unit = {
        LOGGER.debug(s"name: ${metric.fullName}, header: ${header}, row: ${row}")
        val appPath: String = Paths.get(this.s3Location.path,".tmp", metric.appId).toString
        val metricPath: String = Paths.get(appPath, metric.instance, metric.name, metric.name + "." + timestamp + ".csv").toString;
        val inProgressPath: String = Paths.get(appPath,"IN_PROGRESS").toString;

        val metricS3Location: S3Location = this.s3Location.copy(path = metricPath)
        val inProgressS3Location: S3Location = this.s3Location.copy(path = inProgressPath)

        if(!this.isInit) {
            if (!fileWriter.exists(inProgressS3Location.getUrl)) {
                fileWriter.write(inProgressS3Location.getUrl, "")
            }
            this.isInit = true;
        }

        LOGGER.debug(s"Writing row: ${row} to ${inProgressS3Location.getUrl}")
        if (fileWriter.exists(inProgressS3Location.getUrl)) {
            fileWriter.write(metricS3Location.getUrl, row)
        }
    }
}
