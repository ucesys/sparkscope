package org.apache.spark.metrics.reporter;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.codahale.metrics.Clock;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.amazonaws.services.s3.AmazonS3;

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
public class S3CsvReporter extends AbstractCsvReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3CsvReporter.class);
    final AmazonS3 s3;
    final String bucketName;
    final String metricsDir;
    boolean isInit = false;

    public S3CsvReporter(String directory,
                         Optional<String> region,
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
        LOGGER.info("Using S3CsvReporter");
        this.s3 = AmazonS3ClientBuilder.standard().withRegion(region.orElseThrow(() -> new IllegalArgumentException("s3 region is unset!"))).build();

        List<String> dirSplit = Arrays.asList(directory.split("/")).stream().filter(x -> !x.isEmpty()).collect(Collectors.toList());
        this.bucketName = dirSplit.get(1);
        this.metricsDir = String.join("/", dirSplit.subList(2, dirSplit.size()));
        LOGGER.info("bucketName: " + bucketName + ", metricsDir: " + metricsDir);
    }

    protected void report(long timestamp, String name, String header, String line, Object... values) {
        if(!s3.doesBucketExistV2(bucketName)) {
            throw new IllegalArgumentException(bucketName + " bucket does not exist, provided s3 url: " + directory);
        }
        final String nameStripped = name.replace("\"", "").replace("\'", "");
        final List<String> nameSplit = Arrays.asList(nameStripped.split("\\."));
        LOGGER.debug("name: " + name + ", nameStripped: " + nameStripped + ", nameSplit: " + nameSplit);

        final String appId = nameSplit.get(0);
        final String instanceName = nameSplit.get(1);
        final String metricsName = String.join(".", nameSplit.subList(2, nameSplit.size()));

        final String rawPath = Paths.get(
            metricsDir,
            ".tmp",
            appId,
            instanceName,
            metricsName,
            metricsName + "." + timestamp + ".csv"
        ).toString();

        String inProgressPath = Paths.get(
            metricsDir,
            ".tmp",
            appId,
            "IN_PROGRESS"
        ).toString();

        if(!this.isInit) {
            if (!s3.doesObjectExist(bucketName, inProgressPath)) {
                s3.putObject(bucketName, inProgressPath, "");
            }
            this.isInit = true;
        }

        String row = String.format(locale, String.format(locale, "%d" + separator + "%s%n", timestamp, line), values);

        if(s3.doesObjectExist(bucketName, inProgressPath)) {
            s3.putObject(bucketName, rawPath, row);
        }
    }
}
