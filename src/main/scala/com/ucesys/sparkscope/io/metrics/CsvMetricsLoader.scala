package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.timeline.ExecutorTimeline

class CsvMetricsLoader(metricReader: MetricReader)(implicit logger: SparkScopeLogger) extends MetricsLoader {
    def load(appContext: AppContext, sparkScopeConf: SparkScopeConf): DriverExecutorMetrics = {
        logger.info(s"Reading driver metrics from ${sparkScopeConf.driverMetricsDir}, executor metrics from ${sparkScopeConf.executorMetricsDir}")
        val driverMetrics: DataTable = metricReader.readDriver
        // Filter out executorId="driver" which occurs in local mode
        val executorsMetricsMapNonDriver: Map[String, ExecutorTimeline] = appContext.executorMap
          .filter { case (executorId, _) => executorId != "driver" }

        val executorsMetricsMap: Map[String, DataTable] = executorsMetricsMapNonDriver.map { case (executorId, _) =>
            logger.info(s"Reading metrics for executor=${executorId}")
            val metricOpt: Option[DataTable] = try {
                Option(metricReader.readExecutor(executorId))
            }
            catch {
                case ex: Exception => logger.warn(s"Couldn't load metrics for ${executorId}. ${ex}"); None
            }

            metricOpt match {
                case Some(metricTable) => (executorId, Some(metricTable))
                case None => logger.warn(s"Missing metrics for executor=${executorId}"); (executorId, None)
            }
        }.filter { case (_, opt) => opt.nonEmpty }.map { case (id, opt) => (id, opt.get) }

        if (executorsMetricsMap.isEmpty) {
            throw new IllegalArgumentException("No executor metrics found")
        }

        DriverExecutorMetrics(driverMetrics, executorsMetricsMap)
    }
}
