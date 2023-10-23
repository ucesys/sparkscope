package com.ucesys.sparkscope.metrics

import com.ucesys.sparklens.common.ApplicationInfo
import com.ucesys.sparkscope.warning.Warning

case class SparkScopeResult(appInfo: ApplicationInfo,
                            logs: String,
                            warnings: Seq[Warning],
                            stats: SparkScopeStats,
                            metrics: SparkScopeMetrics)
