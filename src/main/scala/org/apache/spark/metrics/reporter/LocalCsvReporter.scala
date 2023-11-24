package org.apache.spark.metrics.reporter

import com.codahale.metrics._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io.{File, FileOutputStream, IOException, OutputStreamWriter, PrintWriter}
import java.util.Locale
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.nio.charset.StandardCharsets.UTF_8

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class LocalCsvReporter(directory: String,
                       registry: MetricRegistry,
                       locale: Locale,
                       separator: String,
                       rateUnit: TimeUnit,
                       durationUnit: TimeUnit,
                       clock: Clock,
                       filter: MetricFilter,
                       executor: ScheduledExecutorService,
                       shutdownExecutorOnStop: Boolean,
                       csvFileProvider: CsvFileProvider)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop) {

    /**
     * A builder for {@link LocalCsvReporter} instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */

    private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)
    LOGGER.info("Using LocalCsvReporter")

    private val directoryFile: File = new File(directory.replace("file:", ""))

    override protected[reporter] def report(timestamp: Long, name: String, header: String, line: String, values: Any*): Unit = {
        try {
            LOGGER.debug(s"Writing to ${directoryFile.getPath}/${name}")
            val file: File = csvFileProvider.getFile(directoryFile, name)
            val fileAlreadyExists: Boolean = file.exists()
            if (fileAlreadyExists || file.createNewFile()) {
                val out: PrintWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true), UTF_8))
                try {
                    if (!fileAlreadyExists) {
                        out.println("t" + separator + header)
                    }
                    val row = formatRow(timestamp, line, values)
                    LOGGER.debug(s"timestamp: ${timestamp}, name: ${name}, header: ${header}, line: ${line}, values: ${values}")
                    LOGGER.debug(s"Writing row: ${row}")
                    out.println(row);
                } finally {
                    out.close()
                }
            }
        } catch {
            case e: IOException => LOGGER.warn(s"Error writing ${name} to local dir ${directory}. ${e}")
        }
    }
}
