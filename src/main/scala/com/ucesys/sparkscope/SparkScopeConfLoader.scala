package com.ucesys.sparkscope

import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.common.{MemorySize, SparkScopeConf, SparkScopeLogger}
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

        val driverMemOverhead: MemorySize = getMemoryOverhead(
            sparkConf,
            SparkScopePropertyDriverMem,
            SparkScopePropertyDriverMemOverhead,
            SparkScopePropertyDriverMemOverheadFactor
        )

        val executorMemOverhead: MemorySize = getMemoryOverhead(
            sparkConf,
            SparkScopePropertyExecMem,
            SparkScopePropertyExecMemOverhead,
            SparkScopePropertyExecMemOverheadFactor
        )

        SparkScopeConf(
            driverMetricsDir = driverMetricsDir.get,
            executorMetricsDir = executorMetricsDir.get,
            htmlReportPath = sparkConf.get(SparkScopePropertyHtmlPath, "/tmp/"),
            appName = sparkConf.getOption(SparkPropertyMetricsConfAppName),
            region = sparkConf.getOption(SparkPropertyMetricsConfS3Region),
            driverMemOverhead = driverMemOverhead,
            executorMemOverhead = executorMemOverhead,
            sparkConf = sparkConf
        )
    }

    def getMemoryOverhead(sparkConf: SparkConf,
                          memoryPropName: String,
                          memoryOverheadPropName: String,
                          memoryOverheadFactorPropName: String): MemorySize = {
        val memoryProp: Option[MemorySize] = sparkConf.getOption(memoryPropName).map(MemorySize.fromStr)
        val memOverheadProp: Option[MemorySize] = sparkConf.getOption(memoryOverheadPropName).map(MemorySize.fromStr)

        // Default memory overhead
        val memOverheadFactor: Float = try {
            sparkConf.getOption(memoryOverheadFactorPropName).map(_.toFloat).getOrElse(0.1f)
        } catch {
            case ex: Exception =>
                logger.warn(s"Could not parse ${memoryOverheadFactorPropName}. " + ex)
                0.1f
        }
        val minimumOverheadInMb: MemorySize = MemorySize.fromMegaBytes(384)
        val memOverheadDefault = memoryProp.map(_.multiply(memOverheadFactor)).map(_.max(minimumOverheadInMb)).getOrElse(minimumOverheadInMb)

        val memoryOverhead: MemorySize = memOverheadProp match {
            case Some(memSize) if memSize > minimumOverheadInMb => memSize
            case Some(memSize) => minimumOverheadInMb
            case None => memOverheadDefault
        }
        memoryOverhead
    }
}

object SparkScopeConfLoader {
    val MetricsPropDriverDir = "driver.sink.csv.directory"
    val MetricsPropExecutorDir = "executor.sink.csv.directory"
    val MetricsPropAllDir = "*.sink.csv.directory"
    val MetricsPropAppName = "*.sink.csv.appName"
    val MetricsPropS3Region = "*.sink.csv.region"

    // Properties used by sinks and sparkscope
    val SparkPropertyMetricsConf = "spark.metrics.conf"
    val SparkPropertyMetricsConfDriverDir = s"${SparkPropertyMetricsConf}.${MetricsPropDriverDir}"
    val SparkPropertyMetricsConfExecutorDir = s"${SparkPropertyMetricsConf}.${MetricsPropExecutorDir}"
    val SparkPropertyMetricsConfAllDir = s"${SparkPropertyMetricsConf}.${MetricsPropAllDir}"
    val SparkPropertyMetricsConfAppName = s"${SparkPropertyMetricsConf}.${MetricsPropAppName}"
    val SparkPropertyMetricsConfS3Region = s"${SparkPropertyMetricsConf}.${MetricsPropS3Region}"

    // Properties used by sparkscope
    val SparkScopePropertyExecutorMetricsDir = "spark.sparkscope.metrics.dir.executor"
    val SparkScopePropertyDriverMetricsDir = "spark.sparkscope.metrics.dir.driver"
    val SparkScopePropertyHtmlPath = "spark.sparkscope.html.path"
    val SparkScopePropertyDriverMem = "spark.driver.memory"
    val SparkScopePropertyDriverMemOverhead = "spark.driver.memoryOverhead"
    val SparkScopePropertyDriverMemOverheadFactor = "spark.driver.memoryOverheadFactor"
    val SparkScopePropertyExecMem = "spark.executor.memory"
    val SparkScopePropertyExecMemOverhead = "spark.executor.memoryOverhead"
    val SparkScopePropertyExecMemOverheadFactor = "spark.executor.memoryOverheadFactor"
}
