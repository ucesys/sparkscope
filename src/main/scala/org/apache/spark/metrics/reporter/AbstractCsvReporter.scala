package org.apache.spark.metrics.reporter

import com.codahale.metrics._
import com.ucesys.sparkscope.common.MetricType.{AllMetricsDriver, AllMetricsExecutor}
import com.ucesys.sparkscope.common.{SparkScopeLogger, SparkScopeMetric}
import com.ucesys.sparkscope.data.DataTable

import java.util.Locale
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
abstract class AbstractCsvReporter(registry: MetricRegistry,
                                   locale: Locale,
                                   separator: String,
                                   rateUnit: TimeUnit,
                                   durationUnit: TimeUnit,
                                   clock: Clock,
                                   filter: MetricFilter,
                                   executor: ScheduledExecutorService,
                                   shutdownExecutorOnStop: Boolean)
                                  (implicit logger: SparkScopeLogger) extends ScheduledReporter(registry, "csv-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop) {

    val DriverHeader: String = (Seq("t") ++ AllMetricsDriver.map(_.name)).mkString(separator)
    val ExecutorHeader: String = (Seq("t") ++ AllMetricsExecutor.map(_.name)).mkString(separator)

    @Override
    def report(gauges: java.util.SortedMap[String, Gauge[_]],
               counters: java.util.SortedMap[String, Counter],
               histograms: java.util.SortedMap[String, Histogram],
               meters: java.util.SortedMap[String, Meter],
               timers: java.util.SortedMap[String, Timer]): Unit = {
        val timestamp: Long  = TimeUnit.MILLISECONDS.toSeconds(clock.getTime)

        report(timestamp, gauges, counters)
    }

    def report(timestamp: Long, gauges: java.util.SortedMap[String, Gauge[_]], counters: java.util.SortedMap[String, Counter]): Unit = {
        val gaugeMetrics: Seq[SparkScopeMetric] = gauges.asScala.map { case (name, value) => SparkScopeMetric.parse(name, value.getValue.toString, "%s")}.toSeq
        val countMetrics: Seq[SparkScopeMetric] = counters.asScala.map { case (name, value) => SparkScopeMetric.parse(name, value.getCount.toString, "%s")}.toSeq
        val allMetrics = gaugeMetrics ++ countMetrics

        val instance: String = gaugeMetrics.head.instance
        val appId: String = gaugeMetrics.head.appId

        val columns: Seq[String] = (Seq("t") ++ allMetrics.map(_.name))
        val header = columns.mkString(separator)

        if(header != DriverHeader && header != ExecutorHeader) {
            throw new IllegalArgumentException(s"Illegal header: ${header}")
        }

        val values: Seq[String] = (Seq(timestamp.toString) ++ allMetrics.map(_.value))
        val formats: String = (Seq("%s") ++ allMetrics.map(_.format)).mkString(separator)
        val formatStr: String = s"%s".formatLocal(locale, formats)
        val row: String = formatStr.formatLocal(locale, values: _*)
        logger.debug(s"header: ${header}, values: ${values}, formats: ${formats}, formatStr: ${formatStr}, row: ${row}", this.getClass)

        val metricsTable = DataTable.fromCsvWithoutHeader(s"${appId}.${instance}", row, separator, columns)
        logger.debug("\n" + metricsTable.toString, this.getClass)

        report(gaugeMetrics.head.appId, gaugeMetrics.head.instance, metricsTable, timestamp)
    }
    protected[reporter] def report(appId: String, instance: String, metrics: DataTable, timestamp: Long): Unit = ???
}
