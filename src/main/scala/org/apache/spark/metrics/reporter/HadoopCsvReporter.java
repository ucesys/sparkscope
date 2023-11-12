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
public class HadoopCsvReporter extends AbstractCsvReporter {

    private static final Logger LOGGER = LoggerFactory.getLogger(HadoopCsvReporter.class);
    private FileSystem fs;

    protected HadoopCsvReporter(String directory,
                                MetricRegistry registry,
                                Locale locale,
                                String separator,
                                TimeUnit rateUnit,
                                TimeUnit durationUnit,
                                Clock clock,
                                MetricFilter filter,
                                ScheduledExecutorService executor,
                                boolean shutdownExecutorOnStop) throws IOException {
        super(registry, locale, directory, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop);
        LOGGER.info("Using HadoopCsvReporter");

        final Configuration configuration = SparkHadoopUtil.get().newConfiguration(null);
        try {
            fs = FileSystem.get(new URI(directory), configuration);
        } catch (URISyntaxException e) {
            LOGGER.warn("URISyntaxException while creating filesystem for directory {}", directory, e);
            fs = FileSystem.get(configuration);
        } catch (IOException e) {
            LOGGER.warn("IOException while creating filesystem for directory {}", directory, e);
            fs = FileSystem.get(configuration);
        }

    }

    protected void report(long timestamp, String name, String header, String line, Object... values) {
        final String nameStripped = name.replace("\"", "").replace("\'", "");
        final Path path = new Path(Paths.get(directory,nameStripped + ".csv").toString());

        try {
            if (!fs.exists(path)) {
                try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true), UTF_8))) {
                    writer.write("t" + separator + header + "\n");
                } catch (IOException e) {
                    LOGGER.warn("IOException while creating csv file: {}", path.getName(), e);
                }
            }

            LOGGER.info("Reading hadoop file {}, scheme: {}", path, fs.getScheme());
            try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.append(path)))) {
                String row = String.format(locale, String.format(locale, "%d" + separator + "%s%n", timestamp, line), values);
                writer.write(row);
            } catch (IOException e) {
                LOGGER.warn("IOException while writing row to csv file: {}", path, e);
            }
        } catch (IOException e) {
            LOGGER.warn("IOException while writing {} to {}", name, directory, e);
        }
    }
}
