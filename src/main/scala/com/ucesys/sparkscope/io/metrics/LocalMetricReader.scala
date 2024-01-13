package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.file.LocalFileReader

import java.nio.file.Paths

class LocalMetricReader(sparkScopeConf: SparkScopeConf, fileReader: LocalFileReader, appContext: SparkScopeContext)
                       (implicit logger: SparkScopeLogger)extends MetricReader {
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
        logger.info(s"Reading instance=${instance} metric files from ${metricPath}")
        val metricStr = fileReader.read(metricPath)
        DataTable.fromCsv(instance, metricStr, ",").distinct("t").sortBy("t")
    }
}
