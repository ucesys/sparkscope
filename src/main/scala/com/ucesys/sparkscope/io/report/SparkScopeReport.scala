package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{AppContext, SparkScopeConf}
import com.ucesys.sparkscope.stats.SparkScopeStats
import com.ucesys.sparkscope.view.chart.SparkScopeCharts

case class SparkScopeReport(appContext: AppContext,
                            conf: SparkScopeConf,
                            stats: SparkScopeStats,
                            charts: SparkScopeCharts,
                            warnings: Seq[String])
