package org.apache.spark.metrics.reporter

import com.codahale.metrics.{Clock, MetricFilter, MetricRegistry}
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.metrics.S3Location
import com.ucesys.sparkscope.io.writer.S3FileWriter

import java.nio.file.Paths
import java.util.Locale
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import collection.mutable.Buffer
/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class BufferedS3CsvReporter(directory: String,
                            registry: MetricRegistry,
                            locale: Locale,
                            separator: String,
                            rateUnit: TimeUnit,
                            durationUnit: TimeUnit,
                            clock: Clock,
                            filter: MetricFilter,
                            executor: ScheduledExecutorService,
                            shutdownExecutorOnStop: Boolean,
                            fileWriter: S3FileWriter,
                            appName: Option[String] = None)
                           (implicit logger: SparkScopeLogger)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop) {
    val s3Location: S3Location = S3Location(directory)
    val metricsBuffer: Buffer[String] = Buffer.empty
    var instance: String = null
    var appId: String = null

    override protected[reporter] def report(appId: String, instance: String, metrics: DataTable, timestamp: Long): Unit = {
        this.instance = instance
        this.appId = appId
        val row: String = metrics.toCsvNoHeader(separator)
        logger.info(s"Appending instance metric row(${row}) to buffer", this.getClass)
        metricsBuffer.append(row)
    }

    override def stop(): Unit = {
        val metricPath: String = Paths.get(this.s3Location.path, this.appName.getOrElse(""), this.appId, s"${this.instance}.csv").toString;
        val metricS3Location: S3Location = this.s3Location.copy(path = metricPath)

        logger.info(s"Writing buffered metric to ${metricS3Location.getUrl}", this.getClass)
        fileWriter.write(metricS3Location.getUrl, metricsBuffer.mkString("\n"))
        super.stop()
    }
}
