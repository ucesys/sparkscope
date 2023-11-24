package org.apache.spark.metrics.reporter;

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.codahale.metrics.Clock
import com.codahale.metrics.MetricFilter
import com.codahale.metrics.MetricRegistry
import org.slf4j.LoggerFactory

import java.util.Locale
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import com.amazonaws.services.s3.AmazonS3
import com.ucesys.sparkscope.io.metrics.S3Location;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
abstract class S3CsvReporter(directory: String,
                             region: Option[String],
                             registry: MetricRegistry,
                             locale: Locale,
                             separator: String,
                             rateUnit: TimeUnit,
                             durationUnit: TimeUnit,
                             clock: Clock,
                             filter: MetricFilter,
                             executor: ScheduledExecutorService,
                             shutdownExecutorOnStop: Boolean)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop) {
    LoggerFactory.getLogger(this.getClass).info("Using S3BufferedCsvReporter");

    val s3: AmazonS3 = AmazonS3ClientBuilder.standard().withRegion(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!"))).build();
    val s3Location: S3Location = S3Location(directory)
}
