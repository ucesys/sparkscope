package org.apache.spark.metrics.reporter;

import com.codahale.metrics.Clock
import com.codahale.metrics.MetricFilter
import com.codahale.metrics.MetricRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.util.Locale
import java.util.concurrent.{ScheduledExecutorService, TimeUnit};

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class S3BufferedCsvReporter(directory: String,
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
  extends S3CsvReporter(directory: String,
      region: Option[String],
      registry: MetricRegistry,
      locale: Locale,
      separator: String,
      rateUnit: TimeUnit,
      durationUnit: TimeUnit,
      clock: Clock,
      filter: MetricFilter,
      executor: ScheduledExecutorService,
      shutdownExecutorOnStop: Boolean) {

    val LOGGER: Logger = LoggerFactory.getLogger(this.getClass);
    LOGGER.info("Using S3BufferedCsvReporter");
        override protected[reporter] def report(timestamp: Long , name: String, header: String, line: String, values: Any*): Unit = {
            ???
        }

//    @Override
//    public void close() {
//        super.close();
//    }
}
