package org.apache.spark.metrics.reporter;

import com.codahale.metrics.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.deploy.SparkHadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
public class HdfsCsvReporter extends AbstractCsvReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsCsvReporter.class);

    protected HdfsCsvReporter(String directory,
                              MetricRegistry registry,
                              Locale locale,
                             String separator,
                             TimeUnit rateUnit,
                             TimeUnit durationUnit,
                             Clock clock,
                             MetricFilter filter,
                             ScheduledExecutorService executor,
                             boolean shutdownExecutorOnStop) {
        super(registry, locale, directory, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop);
        LOGGER.info("Using HdfsCsvReporter");
    }

    protected void report(long timestamp, String name, String header, String line, Object... values) {
        final String nameStripped = name.replace("\"", "").replace("\'", "");
        final Path path = new Path(Paths.get(directory,nameStripped + ".csv").toString());
        final Configuration  configuration = SparkHadoopUtil.get().newConfiguration(null);

        try(FileSystem fs = FileSystem.get(new URI(directory), configuration)) {
            if (!fs.exists(path)) {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true), UTF_8))) {
                    writer.write("t" + separator + header);
                } catch (IOException e) {
                    LOGGER.warn("IOException while creating csv file: {}", path.getName(), e);
                }
            }

            LOGGER.info("Reading hadoop file {}, scheme: {}", path, fs.getScheme());
            try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.append(path)))) {
                writer.write(String.format(locale, "%d" + separator + "%s%n", timestamp, line));
            } catch (IOException e) {
                LOGGER.warn("IOException while writing row to csv file: {}", path, e);
            }
        } catch (IOException e) {
            LOGGER.warn("IOException while writing {} to {}", name, directory, e);
        } catch (URISyntaxException e) {
            LOGGER.warn("URISyntaxException while writing {} to {}", name, directory, e);
        }
    }
}
