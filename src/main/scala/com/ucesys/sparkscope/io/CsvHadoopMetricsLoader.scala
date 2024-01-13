package com.ucesys.sparkscope.io

import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparklens.timespan.ExecutorTimeSpan
import com.ucesys.sparkscope.SparkScopeAnalyzer.{DriverCsvMetrics, ExecutorCsvMetrics}
import com.ucesys.sparkscope.SparkScopeConfig
import com.ucesys.sparkscope.data.DataFrame
import com.ucesys.sparkscope.utils.Logger

import scala.collection.mutable

class CsvHadoopMetricsLoader(readerFactory: FileReaderFactory,
                             appContext: AppContext,
                             sparkScopeConf: SparkScopeConfig) extends MetricsLoader {
  val log = new Logger

  def load(): DriverExecutorMetrics = {
    val ac = appContext.filterByStartAndEndTime(appContext.appInfo.startTime, appContext.appInfo.endTime)

    log.info(s"Reading driver metrics from ${sparkScopeConf.driverMetricsDir}, executor metrics from ${sparkScopeConf.executorMetricsDir}")

    log.info("Reading driver metrics...")
    val driverMetrics: Seq[DataFrame] = DriverCsvMetrics.map { metric =>
      val metricsFilePath = s"${sparkScopeConf.driverMetricsDir}/${appContext.appInfo.applicationID}.driver.${metric}.csv"
      val csvFileStr = readerFactory.getFileReader(metricsFilePath).read(metricsFilePath).replace("value", metric)
      log.info(s"Reading ${metric} metric for driver from " + metricsFilePath)
      DataFrame.fromCsv(metric, csvFileStr, ",")
    }

    if (driverMetrics.map(_.numRows).toSet.size > 1) {
        log.warn(s"Csv files for driver have different rows amounts! Probably a process is still writing to csvs.")
        log.warn(s"Removing extra rows from csv files.")
    }

    val driverMetricsAlignedSize = driverMetrics.map(_.numRows).min
    if (driverMetrics.map(_.numRows).toSet.size > 1) {
      log.warn(s"Csv files for driver have different rows amounts! Probably a process is still writing to csvs.")
      log.warn(s"Removing extra rows from csv files.")
    }
    val driverMetricsAligned =  driverMetrics.map {
      case metric if (metric.numRows == driverMetricsAlignedSize) => metric
      case metric =>
        log.warn(s"Csv files for driver have different rows amounts! Aligning ${metric.name}")
        metric.dropNLastRows(metric.numRows-driverMetricsAlignedSize)
    }

    log.info("Reading executor metrics...")

    // Filter out executorId="driver" which occurs in local mode
    val executorsMetricsMapNonDriver: mutable.HashMap[String, ExecutorTimeSpan] = ac.executorMap
        .filter {case (executorId, _) => executorId != "driver"}

    val executorsMetricsMap: Map[String, Seq[DataFrame]] = executorsMetricsMapNonDriver.map { case (executorId, _) =>
      val metricTables: Seq[DataFrame] = ExecutorCsvMetrics.flatMap { metric =>
        val metricsFilePath = s"${sparkScopeConf.executorMetricsDir}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        log.info(s"Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
        val csvFileStrOpt = try {
          Some(readerFactory.getFileReader(metricsFilePath).read(metricsFilePath).replace("value", metric).replace("count", metric))
        }
        catch {
          case ex: Exception =>
            log.warn(s"Couldn't load ${metricsFilePath}. ${ex}" )
            None
        }
        csvFileStrOpt.map(csvStr => DataFrame.fromCsv(metric, csvStr, ",").distinct("t").sortBy("t"))
      }

      metricTables match {
        case seq: Seq[DataFrame] if (seq.length == ExecutorCsvMetrics.length) => (executorId, Some(metricTables))
        case _ => log.warn(s"Missing metrics for executor=${executorId}");(executorId, None)
      }
    }.retain{case(_, opt) => opt.nonEmpty}.map{case(id, opt) => (id, opt.get)}.toMap

    if(executorsMetricsMap.isEmpty) {
      throw new IllegalArgumentException("No executor metrics found")
    }

    val executorsMetricsMapAligned = executorsMetricsMap.map{case (executorId, metrics) => {
      val executorMetricsAlignedSize = metrics.map(_.numRows).min
      val metricsAligned = metrics.map {
        case metric if (metric.numRows == executorMetricsAlignedSize) => metric
        case metric =>
          log.warn(s"Csv files for executorId=${executorId} have different rows amounts! Aligning ${metric.name}")
          metric.dropNLastRows(metric.numRows-executorMetricsAlignedSize)
      }
      (executorId, metricsAligned)
    }}

    DriverExecutorMetrics(driverMetricsAligned, executorsMetricsMapAligned)
  }
}
