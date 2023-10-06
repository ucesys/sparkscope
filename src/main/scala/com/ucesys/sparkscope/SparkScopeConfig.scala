package com.ucesys.sparkscope

import com.ucesys.sparkscope.io.PropertiesLoaderFactory
import com.ucesys.sparkscope.utils.Logger
import org.apache.spark.SparkConf

case class SparkScopeConfig(driverMetricsDir: String, executorMetricsDir: String, htmlReportPath: String, sparkConf: SparkConf)

object SparkScopeConfig {
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

    val log = new Logger

    def fromSparkConf(sparkConf: SparkConf, propertiesLoaderFactory: PropertiesLoaderFactory): SparkScopeConfig = {
        val driverMetricsDir: Option[String] = sparkConf match {
            case sparkConf if sparkConf.contains(SparkScopePropertyDriverMetricsDir) =>
                log.info(s"Setting driver metrics dir to ${SparkScopePropertyDriverMetricsDir}")
                sparkConf.getOption(SparkScopePropertyDriverMetricsDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfDriverDir) =>
                log.info(s"Setting driver metrics dir to ${SparkPropertyMetricsConfDriverDir}")
                sparkConf.getOption(SparkPropertyMetricsConfDriverDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfAllDir) =>
                log.info(s"Setting driver metrics dir to ${SparkPropertyMetricsConfAllDir}")
                sparkConf.getOption(SparkPropertyMetricsConfAllDir)
            case _ =>
                try {
                    log.info(s"Extracting driver metrics dir from ${SparkPropertyMetricsConf} file")
                    val props = propertiesLoaderFactory.getPropertiesLoader(sparkConf.get(SparkPropertyMetricsConf)).load()
                    Option(props.getProperty(MetricsPropDriverDir, props.getProperty(MetricsPropAllDir)))
                } catch {
                    case ex: Exception =>
                        log.error(s"Loading metrics.properties from ${SparkPropertyMetricsConf} failed. " + ex, ex)
                        None
                }
        }

        val executorMetricsDir: Option[String] = sparkConf match {
            case sparkConf if sparkConf.contains(SparkScopePropertyExecutorMetricsDir) =>
                log.info(s"Setting executor metrics dir to ${SparkScopePropertyExecutorMetricsDir}")
                sparkConf.getOption(SparkScopePropertyExecutorMetricsDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfExecutorDir) =>
                log.info(s"Setting executor metrics dir to ${SparkPropertyMetricsConfExecutorDir}")
                sparkConf.getOption(SparkPropertyMetricsConfExecutorDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfAllDir) =>
                log.info(s"Setting executor metrics dir to ${SparkPropertyMetricsConfAllDir}")
                sparkConf.getOption(SparkPropertyMetricsConfAllDir)
            case _ =>
                try {
                    log.info(s"Extracting executor metrics dir from ${SparkPropertyMetricsConf} file")
                    val props = propertiesLoaderFactory.getPropertiesLoader(sparkConf.get(SparkPropertyMetricsConf)).load()
                    Option(props.getProperty(MetricsPropExecutorDir, props.getProperty(MetricsPropAllDir)))
                } catch {
                    case ex: Exception =>
                        log.error(s"Loading metrics.properties from ${SparkPropertyMetricsConf} failed. " + ex, ex)
                        None
                }
        }

        if (driverMetricsDir.isEmpty || executorMetricsDir.isEmpty) {
            throw new IllegalArgumentException("Unable to extract driver & executor csv metrics directories from SparkConf")
        }

        new SparkScopeConfig(
            driverMetricsDir = driverMetricsDir.get,
            executorMetricsDir = executorMetricsDir.get,
            htmlReportPath = sparkConf.get(SparkScopePropertyHtmlPath, "/tmp/"),
            sparkConf = sparkConf
        )
    }
}
