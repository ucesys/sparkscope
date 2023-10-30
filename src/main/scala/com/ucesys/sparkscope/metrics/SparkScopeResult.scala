package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.common.SparkScopeContext
import com.ucesys.sparkscope.warning.Warning

case class SparkScopeResult(appContext: SparkScopeContext, warnings: Seq[Warning], stats: SparkScopeStats, metrics: SparkScopeMetrics)
