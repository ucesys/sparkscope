package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.file.HadoopFileReader
import com.ucesys.sparkscope.io.MetricType

import java.nio.file.Paths

class HadoopMetricReader(sparkScopeConf: SparkScopeConf, fileReader: HadoopFileReader, appContext: SparkScopeContext) extends MetricReader {
    def readDriver(metricType: MetricType): DataTable = {
        val metricPath: String = Paths.get(sparkScopeConf.driverMetricsDir, s"${appContext.appId}.driver.${metricType.name}").toString
        readMetric(metricType, metricPath)
    }

    def readExecutor(metricType: MetricType, executorId: String): DataTable = {
        val metricPath: String = Paths.get(sparkScopeConf.executorMetricsDir, s"${appContext.appId}.${executorId}.${metricType.name}").toString
        readMetric(metricType, metricPath)
    }

    def readMetric(metricType: MetricType, path: String): DataTable = {
        val metricStr = fileReader.read(path)
        val csvFileStr = metricStr.replace("value", metricType.name).replace("count", metricType.name)
        DataTable.fromCsv(metricType.name, csvFileStr, ",").distinct("t").sortBy("t")
    }
}
