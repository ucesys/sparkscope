package com.ucesys.sparkscope.io

import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparkscope.SparkScopeAnalyzer.{DriverCsvMetrics, ExecutorCsvMetrics}
import com.ucesys.sparkscope.SparkScopeConfig
import com.ucesys.sparkscope.data.DataFrame
import com.ucesys.sparkscope.utils.Logger

class CsvHadoopMetricsLoader(readerFactory: FileReaderFactory,
                             appContext: AppContext,
                             sparkScopeConf: SparkScopeConfig) extends MetricsLoader {
  val log = new Logger

  def load(): DriverExecutorMetrics = {
    val ac = appContext.filterByStartAndEndTime(appContext.appInfo.startTime, appContext.appInfo.endTime)

    log.println(s"Reading driver metrics from ${sparkScopeConf.driverMetricsDir}, executor metrics from ${sparkScopeConf.executorMetricsDir}")

    log.println("Reading driver metrics...")
    val driverMetrics: Seq[DataFrame] = DriverCsvMetrics.map { metric =>
      val metricsFilePath = s"${sparkScopeConf.driverMetricsDir}/${appContext.appInfo.applicationID}.driver.${metric}.csv"
      val csvFileStr = readerFactory.getFileReader(metricsFilePath).read(metricsFilePath).replace("value", metric)
      log.println(s"Reading ${metric} metric for driver from " + metricsFilePath)
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

    log.println("Reading executor metrics...")
    val executorsMetricsMap: Map[Int, Seq[DataFrame]] = ac.executorMap.map { case (executorId, _) =>
      val metricTables: Seq[DataFrame] = ExecutorCsvMetrics.flatMap { metric =>
        val metricsFilePath = s"${sparkScopeConf.executorMetricsDir}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        log.println(s"Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
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
        case seq: Seq[DataFrame] if (seq.length == ExecutorCsvMetrics.length) => (executorId.toInt, Some(metricTables))
        case _ => log.warn(s"Missing metrics for executor=${executorId}");(executorId.toInt, None)
      }

    }.retain{case(_, opt) => opt.nonEmpty}.map{case(id, opt) => (id, opt.get)}.toMap

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
