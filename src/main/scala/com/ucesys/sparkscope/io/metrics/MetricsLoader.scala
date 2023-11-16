package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext}

trait MetricsLoader {
    def load(ac: SparkScopeContext, sparkScopeConf: SparkScopeConf): DriverExecutorMetrics
}
