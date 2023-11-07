package org.apache.spark.metrics.reporter;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Locale;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
public class LocalCsvReporter extends AbstractCsvReporter {

    /**
     * A builder for {@link LocalCsvReporter} instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalCsvReporter.class);

    private final CsvFileProvider csvFileProvider;
    private final File directoryFile;

    protected LocalCsvReporter(String directory,
                               MetricRegistry registry,
                               Locale locale,
                               String separator,
                               TimeUnit rateUnit,
                               TimeUnit durationUnit,
                               Clock clock,
                               MetricFilter filter,
                               ScheduledExecutorService executor,
                               boolean shutdownExecutorOnStop,
                               CsvFileProvider csvFileProvider) {
        super(registry, locale, directory, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop);
        LOGGER.info("Using LocalCsvReporter");
        this.csvFileProvider = csvFileProvider;
        this.directoryFile = new File(directory.replace("file:", ""));
    }

    protected void report(long timestamp, String name, String header, String line, Object... values) {
        try {
            final File file = csvFileProvider.getFile(directoryFile, name);
            final boolean fileAlreadyExists = file.exists();
            if (fileAlreadyExists || file.createNewFile()) {
                try (PrintWriter out = new PrintWriter(new OutputStreamWriter(
                        new FileOutputStream(file, true), UTF_8))) {
                    if (!fileAlreadyExists) {
                        out.println("t" + separator + header);
                    }
                    out.printf(locale, String.format(locale, "%d" + separator + "%s%n", timestamp, line), values);
                }
            }
        } catch (IOException e) {
            LOGGER.warn("Error writing {} to local dir {}", name, directory, e);
        }
    }
}
