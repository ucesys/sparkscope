package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, AppContext}

trait MetricsLoader {
    def load(ac: AppContext, sparkScopeConf: SparkScopeConf): DriverExecutorMetrics
}
