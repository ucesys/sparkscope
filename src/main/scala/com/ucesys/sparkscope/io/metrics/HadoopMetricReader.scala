package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.file.HadoopFileReader

import java.nio.file.Paths

class HadoopMetricReader(sparkScopeConf: SparkScopeConf, fileReader: HadoopFileReader, appContext: SparkScopeContext) extends MetricReader {
    def readDriver: DataTable = {
        readMetric("driver")
    }

    def readExecutor(executorId: String): DataTable = {
        readMetric(executorId)
    }

    def readMetric(instance: String): DataTable = {
        val metricPath: String = Paths.get(
            sparkScopeConf.driverMetricsDir,
            sparkScopeConf.appName.getOrElse(""),
            appContext.appId,
            s"${instance}.csv"
        ).toString.replace("\\", "/")
        val metricStr = fileReader.read(metricPath)
        DataTable.fromCsv(instance, metricStr, ",").distinct("t").sortBy("t")
    }
}
