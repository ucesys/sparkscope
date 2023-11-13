package com.ucesys.sparkscope.io.file

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.ucesys.sparkscope.common.SparkScopeConf
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.{Driver, Executor, InstanceType, MetricType}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf

import java.net.URI
import java.nio.file.Paths

//class S3CsvFileReader(driverMetricsDir: String, executorMetricsDir: String) extends MetricReader {
//
//    def this(sparkScopeConf: SparkScopeConf) = this(sparkScopeConf.driverMetricsDir, sparkScopeConf.executorMetricsDir)
//
//    def read(instanceType: InstanceType, metricType: MetricType): DataTable = {
//        val metricStr = instanceType match {
//            case _: Driver.type => readToString(Paths.get(driverMetricsDir, metricType.name))
//            case _: Executor.type => readToString(Paths.get(executorMetricsDir, metricType.name))
//        }
//        val csvFileStr = metricStr.replace("value", metricType.name)
//        DataTable.fromCsv(metricType.name, csvFileStr, ",")
//    }
//    def readToString(pathStr: String): String = {
//        LOGGER.info("Using S3CsvReporter");
//        this.s3 = AmazonS3ClientBuilder.standard().withRegion(region.orElseThrow(() -> new IllegalArgumentException("s3 region is unset!"))).build();
//
//        List < String > dirSplit = Arrays.asList(directory.split("/")).stream().filter(x -> !x.isEmpty()).collect(Collectors.toList());
//        this.bucketName = dirSplit.get(1);
//        this.metricsDir = String.join("/", dirSplit.subList(2, dirSplit.size()));
//        this.appName = appName.orElse("");
//        this.appDir = Paths.get(metricsDir, this.appName).toString();
//        LOGGER.info("bucketName: " + bucketName + ", metricsDir: " + metricsDir + ", appName: " + appName + ", appDir: " + appDir);
//    }
//}
