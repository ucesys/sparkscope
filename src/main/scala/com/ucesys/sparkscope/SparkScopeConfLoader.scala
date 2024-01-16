/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.ucesys.sparkscope

import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.common.{LogLevel, MemorySize, SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.io.property.PropertiesLoaderFactory
import org.apache.spark.SparkConf

class SparkScopeConfLoader(implicit logger: SparkScopeLogger) {
    def load(sparkConf: SparkConf, propertiesLoaderFactory: PropertiesLoaderFactory): SparkScopeConf = {

        val logLevel = sparkConf.getOption(SparkScopePropertyLogLevel).map(LogLevel.fromString).getOrElse(LogLevel.Info)
        logger.level = logLevel
        logger.info(s"Log level set to ${logLevel.toString.toUpperCase}", this.getClass)

        val driverMetricsDir: Option[String] = sparkConf match {
            case sparkConf if sparkConf.contains(SparkScopePropertyDriverMetricsDir) =>
                logger.info(s"Setting driver metrics dir to ${SparkScopePropertyDriverMetricsDir}", this.getClass)
                sparkConf.getOption(SparkScopePropertyDriverMetricsDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfDriverDir) =>
                logger.info(s"Setting driver metrics dir to ${SparkPropertyMetricsConfDriverDir}", this.getClass)
                sparkConf.getOption(SparkPropertyMetricsConfDriverDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfAllDir) =>
                logger.info(s"Setting driver metrics dir to ${SparkPropertyMetricsConfAllDir}", this.getClass)
                sparkConf.getOption(SparkPropertyMetricsConfAllDir)
            case _ =>
                try {
                    logger.info(s"Extracting driver metrics dir from ${SparkPropertyMetricsConf} file", this.getClass)
                    val props = propertiesLoaderFactory.getPropertiesLoader(sparkConf.get(SparkPropertyMetricsConf)).load()
                    Option(props.getProperty(MetricsPropDriverDir, props.getProperty(MetricsPropAllDir)))
                } catch {
                    case ex: Exception =>
                        logger.error(s"Loading metrics.properties from ${SparkPropertyMetricsConf} failed. " + ex, ex, this.getClass)
                        None
                }
        }

        val executorMetricsDir: Option[String] = sparkConf match {
            case sparkConf if sparkConf.contains(SparkScopePropertyExecutorMetricsDir) =>
                logger.info(s"Setting executor metrics dir to ${SparkScopePropertyExecutorMetricsDir}", this.getClass)
                sparkConf.getOption(SparkScopePropertyExecutorMetricsDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfExecutorDir) =>
                logger.info(s"Setting executor metrics dir to ${SparkPropertyMetricsConfExecutorDir}", this.getClass)
                sparkConf.getOption(SparkPropertyMetricsConfExecutorDir)
            case sparkConf if sparkConf.contains(SparkPropertyMetricsConfAllDir) =>
                logger.info(s"Setting executor metrics dir to ${SparkPropertyMetricsConfAllDir}", this.getClass)
                sparkConf.getOption(SparkPropertyMetricsConfAllDir)
            case _ =>
                try {
                    logger.info(s"Extracting executor metrics dir from ${SparkPropertyMetricsConf} file", this.getClass)
                    val props = propertiesLoaderFactory.getPropertiesLoader(sparkConf.get(SparkPropertyMetricsConf)).load()
                    Option(props.getProperty(MetricsPropExecutorDir, props.getProperty(MetricsPropAllDir)))
                } catch {
                    case ex: Exception =>
                        logger.error(s"Loading metrics.properties from ${SparkPropertyMetricsConf} failed. " + ex, ex, this.getClass)
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

        val diagnosticsUrl: Option[String] = sparkConf.getOption(SparkScopePropertyDiagnosticsEnabled).map(_.toLowerCase) match {
            case Some("false") => None
            case Some("true") => Some(DiagnosticsEndpoint)
            case None => Some(DiagnosticsEndpoint)
        }

        SparkScopeConf(
            driverMetricsDir = driverMetricsDir.get,
            executorMetricsDir = executorMetricsDir.get,
            htmlReportPath = sparkConf.getOption(SparkScopePropertyHtmlPath),
            jsonReportPath = sparkConf.getOption(SparkScopePropertyJsonPath),
            jsonReportServer = sparkConf.getOption(SparkScopePropertyJsonServer),
            logPath = sparkConf.get(SparkScopePropertyLogPath, "/tmp/"),
            appName = sparkConf.getOption(SparkPropertyMetricsConfAppName),
            region = sparkConf.getOption(SparkPropertyMetricsConfS3Region),
            diagnosticsUrl = diagnosticsUrl,
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
                logger.warn(s"Could not parse ${memoryOverheadFactorPropName}. " + ex, this.getClass)
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
    val SparkScopePropertyHtmlPath = "spark.sparkscope.report.html.path"
    val SparkScopePropertyJsonPath = "spark.sparkscope.report.json.path"
    val SparkScopePropertyJsonServer = "spark.sparkscope.report.json.server"
    val SparkScopePropertyLogPath = "spark.sparkscope.log.path"
    val SparkScopePropertyLogLevel = "spark.sparkscope.log.level"
    val SparkScopePropertyDiagnosticsEnabled = "spark.sparkscope.diagnostics.enabled"

    val SparkScopePropertyDriverMem = "spark.driver.memory"
    val SparkScopePropertyDriverMemOverhead = "spark.driver.memoryOverhead"
    val SparkScopePropertyDriverMemOverheadFactor = "spark.driver.memoryOverheadFactor"
    val SparkScopePropertyExecMem = "spark.executor.memory"
    val SparkScopePropertyExecMemOverhead = "spark.executor.memoryOverhead"
    val SparkScopePropertyExecMemOverheadFactor = "spark.executor.memoryOverheadFactor"

    val DiagnosticsEndpoint: String = "http://sparkscope.ai/diagnostics"
}
