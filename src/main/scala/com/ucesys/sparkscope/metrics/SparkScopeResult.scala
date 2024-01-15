package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.stats.SparkScopeStats
import com.ucesys.sparkscope.view.chart.SparkScopeCharts
import com.ucesys.sparkscope.view.warning.Warning

case class SparkScopeResult(stats: SparkScopeStats, charts: SparkScopeCharts, warnings: Seq[Warning])
