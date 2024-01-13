package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.AppContext
import com.ucesys.sparkscope.stats.SparkScopeStats

case class DiagnosticsReport(appContext: AppContext, stats: SparkScopeStats)
