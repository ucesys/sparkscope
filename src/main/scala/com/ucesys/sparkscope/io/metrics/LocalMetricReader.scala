package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.reader.LocalFileReader

import java.nio.file.Paths

class LocalMetricReader(sparkScopeConf: SparkScopeConf, fileReader: LocalFileReader, appContext: AppContext)
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
        logger.info(s"Reading instance=${instance} metric files from ${metricPath}", this.getClass)
        val metricStr = fileReader.read(metricPath)
        DataTable.fromCsv(instance, metricStr, ",").distinct("t").sortBy("t")
    }
}
