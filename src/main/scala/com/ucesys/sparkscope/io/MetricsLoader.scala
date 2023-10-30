package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.common.SparkScopeContext
import com.ucesys.sparkscope.common.SparkScopeConf

trait MetricsLoader {
    def load(ac: SparkScopeContext, sparkScopeConf: SparkScopeConf): DriverExecutorMetrics
}
