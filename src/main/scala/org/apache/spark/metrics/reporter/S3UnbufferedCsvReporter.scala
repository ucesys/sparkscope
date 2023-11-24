package org.apache.spark.metrics.reporter;

import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Locale;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class S3UnbufferedCsvReporter(directory: String,
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
    var isInit: Boolean = false;

    override protected[reporter] def report(timestamp: Long , name: String, header: String, line: String, values: Any*): Unit = {
        if(!s3.doesBucketExistV2(this.s3Location.bucketName)) {
            throw new IllegalArgumentException(s"${this.s3Location.bucketName} bucket does not exist, provided s3 url: ${directory}")
        }
        val nameStripped: String  = name.replace("\"", "").replace("\'", "")
        val nameSplit: Seq[String] = nameStripped.split("\\.");
        LOGGER.debug(s"name: ${name}, nameStripped: ${nameStripped}, nameSplit: ${nameSplit}");

        val appId: String  = nameSplit.head;
        val instanceName: String = nameSplit(1);
        val metricsName: String = nameSplit.slice(2, nameSplit.length).mkString(".")

        val rawPath: String = Paths.get(
            this.s3Location.path,
            ".tmp",
            appId,
            instanceName,
            metricsName,
            metricsName + "." + timestamp + ".csv"
        ).toString;

        val inProgressPath: String = Paths.get(
            this.s3Location.path,
            ".tmp",
            appId,
            "IN_PROGRESS"
        ).toString();

        if(!this.isInit) {
            if (!s3.doesObjectExist(this.s3Location.bucketName, inProgressPath)) {
                s3.putObject(this.s3Location.bucketName, inProgressPath, "");
            }
            this.isInit = true;
        }

        val row = formatRow(timestamp, line, values)
        LOGGER.debug(s"Writing row: ${row}")

        if(s3.doesObjectExist(this.s3Location.bucketName, inProgressPath)) {
            s3.putObject(this.s3Location.bucketName, rawPath, row);
        }
    }
}
