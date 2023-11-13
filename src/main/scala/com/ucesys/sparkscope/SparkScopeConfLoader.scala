package com.ucesys.sparkscope

import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.io.property.PropertiesLoaderFactory
import org.apache.spark.SparkConf

class SparkScopeConfLoader(implicit logger: SparkScopeLogger) {
    def load(sparkConf: SparkConf, propertiesLoaderFactory: PropertiesLoaderFactory): SparkScopeConf = {
        val driverMetricsDir: Option[String] = sparkConf match {
            case sparkConf if sparkConf.contains(SparkScopePropertyDriverMetricsDir) =>
                logger.info(s"Setting driver metrics dir to ${SparkScopePropertyDriverMetricsDir}")
                sparkConf.getOption(SparkScopePropertyDriverMetricsDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfDriverDir) =>
                logger.info(s"Setting driver metrics dir to ${SparkPropertyMetricsConfDriverDir}")
                sparkConf.getOption(SparkPropertyMetricsConfDriverDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfAllDir) =>
                logger.info(s"Setting driver metrics dir to ${SparkPropertyMetricsConfAllDir}")
                sparkConf.getOption(SparkPropertyMetricsConfAllDir)
            case _ =>
                try {
                    logger.info(s"Extracting driver metrics dir from ${SparkPropertyMetricsConf} file")
                    val props = propertiesLoaderFactory.getPropertiesLoader(sparkConf.get(SparkPropertyMetricsConf)).load()
                    Option(props.getProperty(MetricsPropDriverDir, props.getProperty(MetricsPropAllDir)))
                } catch {
                    case ex: Exception =>
                        logger.error(s"Loading metrics.properties from ${SparkPropertyMetricsConf} failed. " + ex, ex)
                        None
                }
        }

        val executorMetricsDir: Option[String] = sparkConf match {
            case sparkConf if sparkConf.contains(SparkScopePropertyExecutorMetricsDir) =>
                logger.info(s"Setting executor metrics dir to ${SparkScopePropertyExecutorMetricsDir}")
                sparkConf.getOption(SparkScopePropertyExecutorMetricsDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfExecutorDir) =>
                logger.info(s"Setting executor metrics dir to ${SparkPropertyMetricsConfExecutorDir}")
                sparkConf.getOption(SparkPropertyMetricsConfExecutorDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfAllDir) =>
                logger.info(s"Setting executor metrics dir to ${SparkPropertyMetricsConfAllDir}")
                sparkConf.getOption(SparkPropertyMetricsConfAllDir)
            case _ =>
                try {
                    logger.info(s"Extracting executor metrics dir from ${SparkPropertyMetricsConf} file")
                    val props = propertiesLoaderFactory.getPropertiesLoader(sparkConf.get(SparkPropertyMetricsConf)).load()
                    Option(props.getProperty(MetricsPropExecutorDir, props.getProperty(MetricsPropAllDir)))
                } catch {
                    case ex: Exception =>
                        logger.error(s"Loading metrics.properties from ${SparkPropertyMetricsConf} failed. " + ex, ex)
                        None
                }
        }

        if (driverMetricsDir.isEmpty || executorMetricsDir.isEmpty) {
            throw new IllegalArgumentException("Unable to extract driver & executor csv metrics directories from SparkConf")
        }

        SparkScopeConf(
            driverMetricsDir = driverMetricsDir.get,
            executorMetricsDir = executorMetricsDir.get,
            htmlReportPath = sparkConf.get(SparkScopePropertyHtmlPath, "/tmp/"),
            sparkConf = sparkConf
        )
    }
}

object SparkScopeConfLoader {
    val MetricsPropDriverDir = "driver.sink.csv.directory"
    val MetricsPropExecutorDir = "executor.sink.csv.directory"
    val MetricsPropAllDir = "*.sink.csv.directory"

    val SparkPropertyMetricsConf = "spark.metrics.conf"
    val SparkPropertyMetricsConfDriverDir = s"${SparkPropertyMetricsConf}.${MetricsPropDriverDir}"
    val SparkPropertyMetricsConfExecutorDir = s"${SparkPropertyMetricsConf}.${MetricsPropExecutorDir}"
    val SparkPropertyMetricsConfAllDir = s"${SparkPropertyMetricsConf}.${MetricsPropAllDir}"

    val SparkScopePropertyExecutorMetricsDir = "spark.sparkscope.metrics.dir.executor"
    val SparkScopePropertyDriverMetricsDir = "spark.sparkscope.metrics.dir.driver"
    val SparkScopePropertyHtmlPath = "spark.sparkscope.html.path"
}
