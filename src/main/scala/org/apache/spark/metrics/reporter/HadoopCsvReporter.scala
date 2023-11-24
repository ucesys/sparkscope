package org.apache.spark.metrics.reporter

import com.codahale.metrics._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.deploy.SparkHadoopUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.net.URI
import java.net.URISyntaxException
import java.nio.file.Paths
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import java.util.Locale

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class HadoopCsvReporter(directory: String,
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
    private val LOGGER: Logger = LoggerFactory.getLogger(this.getClass)
    LOGGER.info("Using HadoopCsvReporter")

    val configuration: Configuration = SparkHadoopUtil.get.newConfiguration(null)

    val fs: FileSystem  = try {
        FileSystem.get(new URI(directory), configuration)
    } catch {
        case e: URISyntaxException =>
            LOGGER.warn(s"URISyntaxException while creating filesystem for directory ${directory}. ${e}")
            FileSystem.get(configuration)
        case e: IOException =>
            LOGGER.warn(s"IOException while creating filesystem for directory ${directory}. ${e}")
            FileSystem.get(configuration)
    }

    override protected[reporter] def report(timestamp: Long , name: String, header: String, line: String, values: Any*): Unit = {
        val nameStripped: String = name.replace("\"", "").replace("\'", "")
        val path: Path = new Path(Paths.get(directory,nameStripped + ".csv").toString)

        try {
            if (!fs.exists(path)) {
                val writer: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.create(path, true), UTF_8))
                try {
                    writer.write("t" + separator + header + "\n")
                } catch {
                    case e: IOException => LOGGER.warn(s"IOException while creating csv file: ${path.getName}. ${e}")
                } finally {
                    writer.close()
                }
            }

            LOGGER.debug(s"Reading hadoop file ${path.toString}, scheme: ${fs.getScheme}")
            val writer: BufferedWriter = new BufferedWriter(new OutputStreamWriter(fs.append(path)))

            try {
                val row = formatRow(timestamp, line, values)
                LOGGER.debug(s"Writing row: ${row}")
                writer.write(row + "\n")
            } catch {
                case e: IOException => LOGGER.warn(s"IOException while writing row to csv file: ${path}. ${e}")
            } finally {
                writer.close()
            }
        } catch {
            case e: IOException => LOGGER.warn(s"IOException while writing ${name} to ${directory}. ${e}")
        }
    }
}
