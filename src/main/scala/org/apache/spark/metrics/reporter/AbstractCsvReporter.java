package org.apache.spark.metrics.reporter;

import com.codahale.metrics.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
public abstract class AbstractCsvReporter extends ScheduledReporter {
    protected final String directory;
    protected final Locale locale;
    protected final String separator;
    protected final Clock clock;
    protected final String histogramFormat;
    protected final String meterFormat;
    protected final String timerFormat;

    protected final String timerHeader;
    protected final String meterHeader;
    protected final String histogramHeader;

    protected AbstractCsvReporter(MetricRegistry registry,
                                  Locale locale,
                                String directory,
                                String separator,
                                TimeUnit rateUnit,
                                TimeUnit durationUnit,
                                Clock clock,
                                MetricFilter filter,
                                ScheduledExecutorService executor,
                                boolean shutdownExecutorOnStop) {
        super(registry, "csv-reporter", filter, rateUnit, durationUnit, executor, shutdownExecutorOnStop);
        this.locale = locale;
        this.directory = directory;
        this.separator = separator;
        this.clock = clock;

        this.histogramFormat = String.join(separator, "%d", "%d", "%f", "%d", "%f", "%f", "%f", "%f", "%f", "%f", "%f");
        this.meterFormat = String.join(separator, "%d", "%f", "%f", "%f", "%f", "events/%s");
        this.timerFormat = String.join(separator, "%d", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "%f", "calls/%s", "%s");

        this.timerHeader = String.join(separator, "count", "max", "mean", "min", "stddev", "p50", "p75", "p95", "p98", "p99", "p999", "mean_rate", "m1_rate", "m5_rate", "m15_rate", "rate_unit", "duration_unit");
        this.meterHeader = String.join(separator, "count", "mean_rate", "m1_rate", "m5_rate", "m15_rate", "rate_unit");
        this.histogramHeader = String.join(separator, "count", "max", "mean", "min", "stddev", "p50", "p75", "p95", "p98", "p99", "p999");
    }

    @Override
    @SuppressWarnings("rawtypes")
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

        for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet()) {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet()) {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet()) {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
    }

    protected void reportTimer(long timestamp, String name, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();

        report(timestamp,
                name,
                timerHeader,
                timerFormat,
                timer.getCount(),
                convertDuration(snapshot.getMax()),
                convertDuration(snapshot.getMean()),
                convertDuration(snapshot.getMin()),
                convertDuration(snapshot.getStdDev()),
                convertDuration(snapshot.getMedian()),
                convertDuration(snapshot.get75thPercentile()),
                convertDuration(snapshot.get95thPercentile()),
                convertDuration(snapshot.get98thPercentile()),
                convertDuration(snapshot.get99thPercentile()),
                convertDuration(snapshot.get999thPercentile()),
                convertRate(timer.getMeanRate()),
                convertRate(timer.getOneMinuteRate()),
                convertRate(timer.getFiveMinuteRate()),
                convertRate(timer.getFifteenMinuteRate()),
                getRateUnit(),
                getDurationUnit());
    }

    protected void reportMeter(long timestamp, String name, Meter meter) {
        report(timestamp,
                name,
                meterHeader,
                meterFormat,
                meter.getCount(),
                convertRate(meter.getMeanRate()),
                convertRate(meter.getOneMinuteRate()),
                convertRate(meter.getFiveMinuteRate()),
                convertRate(meter.getFifteenMinuteRate()),
                getRateUnit());
    }

    protected void reportHistogram(long timestamp, String name, Histogram histogram) {
        final Snapshot snapshot = histogram.getSnapshot();

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
                snapshot.get999thPercentile());
    }

    protected void reportCounter(long timestamp, String name, Counter counter) {
        report(timestamp, name, "count", "%d", counter.getCount());
    }

    protected void reportGauge(long timestamp, String name, Gauge<?> gauge) {
        report(timestamp, name, "value", "%s", gauge.getValue());
    }

    protected abstract void report(long timestamp, String name, String header, String line, Object... values);

    protected String sanitize(String name) {
        return name;
    }
}
