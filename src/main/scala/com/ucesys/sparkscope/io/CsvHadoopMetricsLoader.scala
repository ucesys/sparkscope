package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.common.{ExecutorContext, SparkScopeConf, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.SparkScopeAnalyzer.{DriverCsvMetrics, ExecutorCsvMetrics}
import com.ucesys.sparkscope.data.DataTable

class CsvHadoopMetricsLoader(readerFactory: FileReaderFactory)(implicit logger: SparkScopeLogger) extends MetricsLoader {
    def load(appContext: SparkScopeContext, sparkScopeConf: SparkScopeConf): DriverExecutorMetrics = {
        logger.info(s"Reading driver metrics from ${sparkScopeConf.driverMetricsDir}, executor metrics from ${sparkScopeConf.executorMetricsDir}")

        logger.info("Reading driver metrics...")
        val driverMetrics: Seq[DataTable] = DriverCsvMetrics.map { metric =>
            val metricsFilePath = s"${sparkScopeConf.driverMetricsDir}/${appContext.appId}.driver.${metric}.csv"
            logger.info(s"Reading ${metric} metric for driver from " + metricsFilePath)
            val csvReader = readerFactory.getFileReader(metricsFilePath)
            val csvFileStr = csvReader.read(metricsFilePath).replace("value", metric)
            DataTable.fromCsv(metric, csvFileStr, ",")
        }

        if (driverMetrics.map(_.numRows).toSet.size > 1) {
            logger.warn(s"Csv files for driver have different rows amounts! Probably a process is still writing to csvs.")
            logger.warn(s"Removing extra rows from csv files.")
        }

        val driverMetricsAlignedSize = driverMetrics.map(_.numRows).min
        if (driverMetrics.map(_.numRows).toSet.size > 1) {
            logger.warn(s"Csv files for driver have different rows amounts! Probably a process is still writing to csvs.")
            logger.warn(s"Removing extra rows from csv files.")
        }
        val driverMetricsAligned = driverMetrics.map {
            case metric if (metric.numRows == driverMetricsAlignedSize) => metric
            case metric =>
                logger.warn(s"Csv files for driver have different rows amounts! Aligning ${metric.name}")
                metric.dropNLastRows(metric.numRows - driverMetricsAlignedSize)
        }

        logger.info("Reading executor metrics...")

        // Filter out executorId="driver" which occurs in local mode
        val executorsMetricsMapNonDriver: Map[String, ExecutorContext] = appContext.executorMap
          .filter { case (executorId, _) => executorId != "driver" }

        val executorsMetricsMap: Map[String, Seq[DataTable]] = executorsMetricsMapNonDriver.map { case (executorId, _) =>
            val metricTables: Seq[DataTable] = ExecutorCsvMetrics.flatMap { metric =>
                val metricsFilePath = s"${sparkScopeConf.executorMetricsDir}/${appContext.appId}.${executorId}.${metric}.csv"
                logger.info(s"Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
                val csvFileStrOpt = try {
                    Some(readerFactory.getFileReader(metricsFilePath).read(metricsFilePath).replace("value", metric).replace("count", metric))
                }
                catch {
                    case ex: Exception =>
                        logger.warn(s"Couldn't load ${metricsFilePath}. ${ex}")
                        None
                }
                csvFileStrOpt.map(csvStr => DataTable.fromCsv(metric, csvStr, ",").distinct("t").sortBy("t"))
            }

            metricTables match {
                case seq: Seq[DataTable] if (seq.length == ExecutorCsvMetrics.length) => (executorId, Some(metricTables))
                case _ => logger.warn(s"Missing metrics for executor=${executorId}"); (executorId, None)
            }
        }.filter { case (_, opt) => opt.nonEmpty }.map { case (id, opt) => (id, opt.get) }

        if (executorsMetricsMap.isEmpty) {
            throw new IllegalArgumentException("No executor metrics found")
        }

        val executorsMetricsMapAligned = executorsMetricsMap.map { case (executorId, metrics) => {
            val executorMetricsAlignedSize = metrics.map(_.numRows).min
            val metricsAligned = metrics.map {
                case metric if (metric.numRows == executorMetricsAlignedSize) => metric
                case metric =>
                    logger.warn(s"Csv files for executorId=${executorId} have different rows amounts! Aligning ${metric.name}")
                    metric.dropNLastRows(metric.numRows - executorMetricsAlignedSize)
            }
            (executorId, metricsAligned)
        }
        }

        DriverExecutorMetrics(driverMetricsAligned, executorsMetricsMapAligned)
    }
}
