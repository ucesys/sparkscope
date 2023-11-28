package org.apache.spark.metrics.reporter

import com.codahale.metrics.{Clock, MetricFilter, MetricRegistry}
import com.ucesys.sparkscope.common.SparkScopeMetric
import com.ucesys.sparkscope.io.file.S3FileWriter
import com.ucesys.sparkscope.io.metrics.S3Location
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import java.util.Locale
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import collection.mutable.Buffer
import scala.collection.mutable
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
                            fileWriter: S3FileWriter)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, false) {
    val s3Location: S3Location = S3Location(directory)
    val LOGGER: Logger = LoggerFactory.getLogger(this.getClass);

    val metricsMap: mutable.Map[String, Buffer[String]] = mutable.Map.empty

//    override protected[reporter] def report(metric: SparkScopeMetric, header: String, row: String, timestamp: Long): Unit = {
//        LOGGER.info(s"Appending ${metric.fullName} metric row(${row}) to buffer")
//        if(!metricsMap.contains(metric.fullName)) {
//            metricsMap.put(metric.fullName, Buffer(s"t${separator}${header}"))
//        }
//        metricsMap(metric.fullName).append(row)
//    }
//
//    override def stop(): Unit = {
//        metricsMap.foreach{case (metricFullName, buffer) =>
//            val metric = SparkScopeMetric.parse(metricFullName)
//            val metricPath: String = Paths.get(this.s3Location.path, metric.appId, metric.instance, metric.name + ".csv").toString;
//            val metricS3Location: S3Location = this.s3Location.copy(path = metricPath)
//
//            LOGGER.info(s"Writing buffered metric to ${metricS3Location.getUrl}")
//            fileWriter.write(metricS3Location.getUrl, buffer.mkString("\n"))
//        }
//        super.stop()
//    }
}
