package org.apache.spark.metrics.reporter

import com.codahale.metrics.{Clock, MetricFilter, MetricRegistry}
import com.ucesys.sparkscope.common.SparkScopeMetric
import com.ucesys.sparkscope.io.file.LocalFileWriter
import org.slf4j.{Logger, LoggerFactory}

import java.nio.file.Paths
import java.util.Locale
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}
import scala.collection.mutable
import scala.collection.mutable.Buffer

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class BufferedLocalCsvReporter(directory: String,
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
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, false) {
    val LOGGER: Logger = LoggerFactory.getLogger(this.getClass);
    val metricsMap: mutable.Map[String, Buffer[String]] = mutable.Map.empty

//    override protected[reporter] def report(metric: SparkScopeMetric, header: String, row: String, timestamp: Long): Unit = {
//        LOGGER.info(s"Appending ${metric.fullName} metric row(${row}) to buffer")
//        if(!metricsMap.contains(metric.fullName)) {
//            metricsMap.put(metric.fullName, Buffer(s"t${separator}${header}"))
//        }
//        metricsMap(metric.fullName).append(row)
//    }

//    override def stop(): Unit = {
//        LOGGER.info(s"[INSIDE STOP]")
//        metricsMap.foreach{ case (metricFullName, buffer) =>
//            val metric = SparkScopeMetric.parse(metricFullName)
//            val csvFilePath = Paths.get(directory, s"${metric.fullName}.csv").toString
//            LOGGER.info(s"Writing buffered metric to ${csvFilePath}")
//            fileWriter.write(csvFilePath, buffer.mkString("\n"))
//        }
//        super.stop()
//    }

    override def close(): Unit = {
        LOGGER.info(s"[INSIDE CLOSE]")
        super.close()
    }
}
