package org.apache.spark.metrics.reporter

import com.codahale.metrics._
import com.ucesys.sparkscope.common.Metric

import java.util.Locale
import java.util.SortedMap
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.collection.mutable

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
                                   shutdownExecutorOnStop: Boolean
                                  ) extends ScheduledReporter(registry, "csv-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop) {

    val histogramFormat: String = Seq( "%d", "%d", "%f", "%d", "%f", "%f", "%f", "%f", "%f", "%f", "%f").mkString(separator)
    val meterFormat: String = Seq(separator, "%d", "%f", "%f", "%f", "%f", "events/%s").mkString(separator)
    val timerFormat: String = Seq(separator, "%d", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "calls/%s", "%s").mkString(separator)
    val timerHeader: String = Seq(separator, "count", "max", "mean", "min", "stddev", "p50", "p75", "p95", "p98", "p99", "p999", "mean_rate", "m1_rate", "m5_rate", "m15_rate", "rate_unit", "duration_unit").mkString(separator)
    val meterHeader: String = Seq(separator, "count", "mean_rate", "m1_rate", "m5_rate", "m15_rate", "rate_unit").mkString(separator)
    val histogramHeader: String = Seq(separator, "count", "max", "mean", "min", "stddev", "p50", "p75", "p95", "p98", "p99", "p999").mkString(separator)

    @Override
    def report(gauges: java.util.SortedMap[String, Gauge[_]],
               counters: java.util.SortedMap[String, Counter],
               histograms: java.util.SortedMap[String, Histogram],
               meters: java.util.SortedMap[String, Meter],
               timers: java.util.SortedMap[String, Timer]): Unit = {
        val timestamp: Long  = TimeUnit.MILLISECONDS.toSeconds(clock.getTime)

        gauges.entrySet().forEach(entry => reportGauge(timestamp, entry.getKey, entry.getValue))
        counters.entrySet().forEach(entry => reportCounter(timestamp, entry.getKey, entry.getValue))
        histograms.entrySet().forEach(entry => reportHistogram(timestamp, entry.getKey, entry.getValue))
        meters.entrySet().forEach(entry => reportMeter(timestamp, entry.getKey, entry.getValue))
        timers.entrySet().forEach(entry => reportTimer(timestamp, entry.getKey, entry.getValue))
    }

    protected def reportTimer(timestamp: Long, name: String, timer: Timer): Unit = {
        val snapshot: Snapshot = timer.getSnapshot

        report(
            timestamp,
            name,
            timerHeader,
            timerFormat,
            timer.getCount(),
            convertDuration(snapshot.getMax),
            convertDuration(snapshot.getMean),
            convertDuration(snapshot.getMin),
            convertDuration(snapshot.getStdDev),
            convertDuration(snapshot.getMedian),
            convertDuration(snapshot.get75thPercentile),
            convertDuration(snapshot.get95thPercentile),
            convertDuration(snapshot.get98thPercentile),
            convertDuration(snapshot.get99thPercentile),
            convertDuration(snapshot.get999thPercentile),
            convertRate(timer.getMeanRate),
            convertRate(timer.getOneMinuteRate),
            convertRate(timer.getFiveMinuteRate),
            convertRate(timer.getFifteenMinuteRate),
            getRateUnit,
            getDurationUnit
        )
    }

    protected def reportMeter(timestamp: Long, name: String, meter: Meter) {
        report(
            timestamp,
            name,
            meterHeader,
            meterFormat,
            meter.getCount(),
            convertRate(meter.getMeanRate),
            convertRate(meter.getOneMinuteRate),
            convertRate(meter.getFiveMinuteRate),
            convertRate(meter.getFifteenMinuteRate),
            getRateUnit
        )
    }

    protected def reportHistogram(timestamp: Long, name: String, histogram: Histogram) {
        val snapshot: Snapshot = histogram.getSnapshot

        report(timestamp,
                name,
                histogramHeader,
                histogramFormat,
                histogram.getCount(),
                snapshot.getMax(),
                snapshot.getMean(),
                snapshot.getMin(),
                snapshot.getStdDev(),
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile)
    }

    protected def reportCounter(timestamp: Long, name: String, counter: Counter) {
        report(timestamp, name, "count", "%d", counter.getCount)
    }

    protected def reportGauge(timestamp: Long, name: String, gauge: Gauge[_]) {
        this.report(timestamp, name, "value", "%s", gauge.getValue)
    }

    def formatRow(timestamp: Long, line: String, values: Seq[Any]): String = {
        val formatStr = s"%s${separator}%s".formatLocal(locale, timestamp.toString, line)
        formatStr.formatLocal(locale, values: _*)
    }
    protected[reporter] def report(timestamp: Long, name: String, header: String, line: String, values: Any*): Unit = {
        val metric: Metric = Metric.parse(name)
        val row: String = formatRow(timestamp, line, values)
        report(metric, header, row, timestamp)
    }

    protected[reporter] def report(metric: Metric, header: String, row: String, timestamp: Long): Unit
}
