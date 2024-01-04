package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.common.AppContext
import com.ucesys.sparkscope.stats.SparkScopeStats
import com.ucesys.sparkscope.view.warning.Warning

case class SparkScopeResult(appContext: AppContext,
                            warnings: Seq[Warning],
                            stats: SparkScopeStats,
                            metrics: SparkScopeMetrics)
