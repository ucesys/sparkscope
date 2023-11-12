package org.apache.spark.metrics.reporter;

import com.codahale.metrics.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
/**
 * A builder for {@link AbstractCsvReporter} instances. Defaults to using the default locale, converting
 * rates to events/second, converting durations to milliseconds, and not filtering metrics.
 */

public class CsvReporterBuilder {
    private final MetricRegistry registry;
    private Locale locale;
    private String separator;
    private TimeUnit rateUnit;
    private TimeUnit durationUnit;
    private Clock clock;
    private MetricFilter filter;
    private ScheduledExecutorService executor;
    private boolean shutdownExecutorOnStop;
    private CsvFileProvider csvFileProvider;

    private final List<String> SPARK_SCOPE_METRICS = Arrays.asList("jvm.heap.used", "jvm.heap.usage", "jvm.heap.max", "jvm.non-heap.used", "executor.cpuTime");
    MetricFilter SPARK_SCOPE_METRICS_FILTER = (name, metric) -> {
        return SPARK_SCOPE_METRICS.stream().anyMatch(name::contains);
    };

    protected static final String DEFAULT_SEPARATOR = ",";

    /**
     * Returns a new {@link CsvReporterBuilder} for {@link CsvReporterBuilder}.
     *
     * @param registry the registry to report
     * @return a {@link CsvReporterBuilder} instance for a {@link CsvReporterBuilder}
     */
    public static CsvReporterBuilder forRegistry(MetricRegistry registry) {
        return new CsvReporterBuilder(registry);
    }

    private CsvReporterBuilder(MetricRegistry registry) {
        this.registry = registry;
        this.locale = Locale.getDefault();
        this.separator = DEFAULT_SEPARATOR;
        this.rateUnit = TimeUnit.SECONDS;
        this.durationUnit = TimeUnit.MILLISECONDS;
        this.clock = Clock.defaultClock();
        this.filter = SPARK_SCOPE_METRICS_FILTER;
        this.executor = null;
        this.shutdownExecutorOnStop = true;
        this.csvFileProvider = new FixedNameCsvFileProvider();
    }

    /**
     * Specifies whether or not, the executor (used for reporting) will be stopped with same time with reporter.
     * Default value is true.
     * Setting this parameter to false, has the sense in combining with providing external managed executor via {@link #scheduleOn(ScheduledExecutorService)}.
     *
     * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
     * @return {@code this}
     */
    public CsvReporterBuilder shutdownExecutorOnStop(boolean shutdownExecutorOnStop) {
        this.shutdownExecutorOnStop = shutdownExecutorOnStop;
        return this;
    }

    /**
     * Specifies the executor to use while scheduling reporting of metrics.
     * Default value is null.
     * Null value leads to executor will be auto created on start.
     *
     * @param executor the executor to use while scheduling reporting of metrics.
     * @return {@code this}
     */
    public CsvReporterBuilder scheduleOn(ScheduledExecutorService executor) {
        this.executor = executor;
        return this;
    }

    /**
     * Format numbers for the given {@link Locale}.
     *
     * @param locale a {@link Locale}
     * @return {@code this}
     */
    public CsvReporterBuilder formatFor(Locale locale) {
        this.locale = locale;
        return this;
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    public CsvReporterBuilder convertRatesTo(TimeUnit rateUnit) {
        this.rateUnit = rateUnit;
        return this;
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    public CsvReporterBuilder convertDurationsTo(TimeUnit durationUnit) {
        this.durationUnit = durationUnit;
        return this;
    }

    /**
     * Use the given string to use as the separator for values.
     *
     * @param separator the string to use for the separator.
     * @return {@code this}
     */
    public CsvReporterBuilder withSeparator(String separator) {
        this.separator = separator;
        return this;
    }

    /**
     * Use the given {@link Clock} instance for the time.
     *
     * @param clock a {@link Clock} instance
     * @return {@code this}
     */
    public CsvReporterBuilder withClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    public CsvReporterBuilder filter(MetricFilter filter) {
        this.filter = filter;
        return this;
    }

    public CsvReporterBuilder withCsvFileProvider(CsvFileProvider csvFileProvider) {
        this.csvFileProvider = csvFileProvider;
        return this;
    }

    /**
     * Builds a {@link CsvReporterBuilder} with the given properties, writing {@code .csv} files to the
     * given directory.
     *
     * @param directory the directory in which the {@code .csv} files will be created
     * @return a {@link CsvReporterBuilder}
     */
    public AbstractCsvReporter build(String directory, Optional<String> appName, Optional<String> s3Region) throws IOException {
        if (directory.startsWith("maprfs:/") || directory.startsWith("hdfs:/")) {
            return new HadoopCsvReporter(
                    directory,
                    registry,
                    locale,
                    separator,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter,
                    executor,
                    shutdownExecutorOnStop
            );
        } else if (directory.startsWith("s3:/")) {
            return new S3CsvReporter(
                    directory,
                    appName,
                    s3Region,
                    registry,
                    locale,
                    separator,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter,
                    executor,
                    shutdownExecutorOnStop
            );
        } else {
            return new LocalCsvReporter(
                    directory,
                    registry,
                    locale,
                    separator,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter,
                    executor,
                    shutdownExecutorOnStop,
                    csvFileProvider
            );
        }
    }
}
