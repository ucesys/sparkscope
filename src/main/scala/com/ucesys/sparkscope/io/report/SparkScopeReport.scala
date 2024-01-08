package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{AppContext, SparkScopeConf}
import com.ucesys.sparkscope.metrics.SparkScopeResult
import com.ucesys.sparkscope.stats.SparkScopeStats
import com.ucesys.sparkscope.view.chart.SparkScopeCharts

case class SparkScopeReport(appContext: AppContext,
                            sparkConf: Map[String, String],
                            stats: SparkScopeStats,
                            charts: SparkScopeCharts,
                            warnings: Seq[String])

object SparkScopeReport {
    def apply(appContext: AppContext, result: SparkScopeResult, conf: SparkScopeConf): SparkScopeReport = {
        SparkScopeReport(
            appContext = appContext,
            sparkConf = conf.sparkConf.getAll.toMap,
            stats = result.stats,
            charts = result.charts,
            warnings = result.warnings.map(_.toString)
        )
    }
}
