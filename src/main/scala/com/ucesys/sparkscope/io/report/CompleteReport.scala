package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{AppContext, SparkScopeConf}
import com.ucesys.sparkscope.metrics.SparkScopeResult

case class CompleteReport(appContext: AppContext, conf: SparkScopeConf, result: SparkScopeResult)
