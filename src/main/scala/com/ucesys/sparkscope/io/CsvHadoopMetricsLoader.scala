package com.ucesys.sparkscope.io

import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparkscope.SparkScopeAnalyzer.{DriverCsvMetrics, ExecutorCsvMetrics}
import com.ucesys.sparkscope.data.DataFrame
import com.ucesys.sparkscope.utils.Logger
import org.apache.spark.SparkConf

class CsvHadoopMetricsLoader(reader: CsvHadoopReader,
                             appContext: AppContext,
                             sparkConf: SparkConf,
                             propertiesLoader: PropertiesLoader) extends MetricsLoader {
  val log = new Logger


  def getMetricsPropertiesPath() = {
    sparkConf.getAll.foreach(println)
    val sparkHome = sparkConf.get("spark.home", sys.env.getOrElse("SPARK_HOME", sys.env("PWD")))
    val defaultMetricsPropsPath = sparkHome + "/conf/metrics.properties"
    val metricsPropertiesPath = sparkConf.get("spark.metrics.conf", defaultMetricsPropsPath)
    metricsPropertiesPath
  }
  def load(): DriverExecutorMetrics = {
    val ac = appContext.filterByStartAndEndTime(appContext.appInfo.startTime, appContext.appInfo.endTime)

    val metricsPropertiesPath = getMetricsPropertiesPath()

    val prop = propertiesLoader.load(metricsPropertiesPath)

    val driverMetricsDir = Option(prop.getProperty("driver.sink.csv.directory", prop.getProperty("*.sink.csv.directory")))
    val executorMetricsDir = Option(prop.getProperty("executor.sink.csv.directory", prop.getProperty("*.sink.csv.directory")))
    log.println(s"Reading driver metrics from ${driverMetricsDir}, executor metrics from ${executorMetricsDir}")
    if(driverMetricsDir.isEmpty || executorMetricsDir.isEmpty) {
      throw new NoSuchFieldException("metrics.properties didn't contain csv sink configuration!")
    }

    log.println("Reading driver metrics...")
    val driverMetrics: Seq[DataFrame] = DriverCsvMetrics.map { metric =>
      val metricsFilePath = s"${driverMetricsDir.get}/${appContext.appInfo.applicationID}.driver.${metric}.csv"
      val csvFileStr = reader.read(metricsFilePath).replace("value", metric)
      log.println(s"Reading ${metric} metric for driver from " + metricsFilePath)
      DataFrame.fromCsv(metric, csvFileStr, ",")
    }

    if (driverMetrics.map(_.numRows).toSet.size > 1) {
      throw new IllegalArgumentException(
        s"csv files for driver have different rows amounts! Probably a process is still writing to csvs.")
    }

    log.println("Reading executor metrics...")
    val executorsMetricsMap: Map[Int, Seq[DataFrame]] = (0 until ac.executorMap.size).map { executorId =>
      val metricTables: Seq[DataFrame] = ExecutorCsvMetrics.map { metric =>
        val metricsFilePath = s"${executorMetricsDir.get}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        val csvFileStr = reader.read(metricsFilePath).replace("value", metric).replace("count", metric)
        log.println(s"Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
        DataFrame.fromCsv(metric, csvFileStr, ",").distinct("t").sortBy("t")
      }
      (executorId, metricTables)
    }.toMap

    executorsMetricsMap.foreach{case (executorId, metrics) => {
      if(metrics.map(_.numRows).toSet.size > 1) {
        throw new IllegalArgumentException(
          s"csv files for ${executorId} have different rows amounts! Probably a process is still writing to csvs.")
      }
    }}

    DriverExecutorMetrics(driverMetrics, executorsMetricsMap)
  }
}
