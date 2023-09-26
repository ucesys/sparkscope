package com.ucesys.sparkscope

import com.ucesys.sparkscope.io.PropertiesLoaderFactory
import org.apache.spark.SparkConf

case class SparkScopeConfig(driverMetricsDir: String, executorMetricsDir: String, htmlReportPath: String, sparkConf: SparkConf)

object SparkScopeConfig {
    def load(sparkConf: SparkConf, propertiesLoaderFactory: PropertiesLoaderFactory): SparkScopeConfig = {
        val metricsPropertiesPath = getMetricsPropertiesPath(sparkConf)

        val props = propertiesLoaderFactory.getPropertiesLoader(metricsPropertiesPath).load()

        val driverMetricsDirProps = Option(props.getProperty("driver.sink.csv.directory", props.getProperty("*.sink.csv.directory")))
        val executorMetricsDirProps = Option(props.getProperty("executor.sink.csv.directory", props.getProperty("*.sink.csv.directory")))

        if (driverMetricsDirProps.isEmpty || executorMetricsDirProps.isEmpty) {
            throw new NoSuchFieldException("metrics.properties didn't contain csv sink configuration for executor or driver!")
        }

        new SparkScopeConfig(
            driverMetricsDir = sparkConf.get( "spark.sparkscope.metrics.dir.driver ", driverMetricsDirProps.get),
            executorMetricsDir = sparkConf.get( "spark.sparkscope.metrics.dir.executor", executorMetricsDirProps.get),
            htmlReportPath = sparkConf.get("spark.sparkscope.html.path", "/tmp/"),
            sparkConf = sparkConf
        )
    }

    def getMetricsPropertiesPath(sparkConf: SparkConf): String = {
        val sparkHome = sparkConf.get("spark.home", sys.env.getOrElse("SPARK_HOME", "./"))
        val defaultMetricsPropsPath = sparkHome + "/conf/metrics.properties"
        val metricsPropertiesPath = sparkConf.get("spark.metrics.conf", defaultMetricsPropsPath)
        metricsPropertiesPath
    }
}
