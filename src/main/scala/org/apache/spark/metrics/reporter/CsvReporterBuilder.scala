package org.apache.spark.metrics.reporter

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.codahale.metrics._
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.file.{HadoopFileWriter, LocalFileWriter, S3FileWriter}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.metrics.reporter.CsvReporterBuilder.{DEFAULT_SEPARATOR, SPARK_SCOPE_METRICS_FILTER}

import java.util.Locale
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
/**
 * A builder for {@link AbstractCsvReporter} instances. Defaults to using the default locale, converting
 * rates to events/second, converting durations to milliseconds, and not filtering metrics.
 */

class CsvReporterBuilder(val registry: MetricRegistry,
                         var locale: Locale,
                         var separator: String,
                         var rateUnit: TimeUnit,
                         var durationUnit: TimeUnit,
                         var clock: Clock,
                         var filter: MetricFilter,
                         var executor: ScheduledExecutorService,
                         var shutdownExecutorOnStop: Boolean,
                         var csvFileProvider: CsvFileProvider) {

    def this(registry: MetricRegistry) {
        this(
            registry,
            Locale.getDefault(),
            DEFAULT_SEPARATOR,
            TimeUnit.SECONDS,
            TimeUnit.MILLISECONDS,
            Clock.defaultClock(),
            SPARK_SCOPE_METRICS_FILTER,
            null,
            true,
            new FixedNameCsvFileProvider())
    }

    /**
     * Specifies whether or not, the executor (used for reporting) will be stopped with same time with reporter.
     * Default value is true.
     * Setting this parameter to false, has the sense in combining with providing external managed executor via {@link #scheduleOn(ScheduledExecutorService)}.
     *
     * @param shutdownExecutorOnStop if true, then executor will be stopped in same time with this reporter
     * @return {@code this}
     */
    def shutdownExecutorOnStop(shutdownExecutorOnStop: Boolean): CsvReporterBuilder = {
        this.shutdownExecutorOnStop = shutdownExecutorOnStop
        this
    }

    /**
     * Specifies the executor to use while scheduling reporting of metrics.
     * Default value is null.
     * Null value leads to executor will be auto created on start.
     *
     * @param executor the executor to use while scheduling reporting of metrics.
     * @return {@code this}
     */
    def scheduleOn(executor: ScheduledExecutorService): CsvReporterBuilder = {
        this.executor = executor
        this
    }

    /**
     * Format numbers for the given {@link Locale}.
     *
     * @param locale a {@link Locale}
     * @return {@code this}
     */
    def formatFor(locale: Locale): CsvReporterBuilder = {
        this.locale = locale
        this
    }

    /**
     * Convert rates to the given time unit.
     *
     * @param rateUnit a unit of time
     * @return {@code this}
     */
    def convertRatesTo(rateUnit: TimeUnit): CsvReporterBuilder = {
        this.rateUnit = rateUnit
        this
    }

    /**
     * Convert durations to the given time unit.
     *
     * @param durationUnit a unit of time
     * @return {@code this}
     */
    def convertDurationsTo(durationUnit: TimeUnit): CsvReporterBuilder = {
        this.durationUnit = durationUnit
        this
    }

    /**
     * Use the given string to use as the separator for values.
     *
     * @param separator the string to use for the separator.
     * @return {@code this}
     */
    def withSeparator(separator: String): CsvReporterBuilder = {
        this.separator = separator
        this
    }

    /**
     * Use the given {@link Clock} instance for the time.
     *
     * @param clock a {@link Clock} instance
     * @return {@code this}
     */
    def withClock(clock: Clock): CsvReporterBuilder = {
        this.clock = clock
        this
    }

    /**
     * Only report metrics which match the given filter.
     *
     * @param filter a {@link MetricFilter}
     * @return {@code this}
     */
    def filter(filter: MetricFilter): CsvReporterBuilder = {
        this.filter = filter
        this
    }

    def withCsvFileProvider(csvFileProvider: CsvFileProvider): CsvReporterBuilder = {
        this.csvFileProvider = csvFileProvider
        this
    }

    /**
     * Builds a {@link CsvReporterBuilder} with the given properties, writing {@code .csv} files to the
     * given directory.
     *
     * @param directory the directory in which the {@code .csv} files will be created
     * @return a {@link CsvReporterBuilder}
     */
    def build(directory: String, appName: Option[String], s3Region: Option[String]): AbstractCsvReporter = {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger

        if (directory.startsWith("maprfs:/") || directory.startsWith("hdfs:/")) {
            new HadoopCsvReporter(
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
                new HadoopFileWriter(SparkHadoopUtil.get.newConfiguration(null)),
                appName
            )
        } else if (directory.startsWith("s3:/")) {
            new S3CsvReporter(
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
                new S3FileWriter(
                    AmazonS3ClientBuilder.standard().withRegion(s3Region.getOrElse(throw new IllegalArgumentException("s3 region is unset!"))).build()
                )
            )
        } else {
            new LocalCsvReporter(
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
                new LocalFileWriter,
                appName
            )
        }
    }
}

object CsvReporterBuilder {
    val DEFAULT_SEPARATOR = ","

    /**
     * Returns a new {@link CsvReporterBuilder} for {@link CsvReporterBuilder}.
     *
     * @param registry the registry to report
     * @return a {@link CsvReporterBuilder} instance for a {@link CsvReporterBuilder}
     */
   def forRegistry (registry: MetricRegistry): CsvReporterBuilder = {
        new CsvReporterBuilder(registry)
    }

    val SPARK_SCOPE_METRICS: Seq[String] = Seq("jvm.heap.used", "jvm.heap.usage", "jvm.heap.max", "jvm.non-heap.used", "executor.cpuTime")

    val SPARK_SCOPE_METRICS_FILTER: MetricFilter = (name, _) => SPARK_SCOPE_METRICS.exists(name.contains(_))
}
