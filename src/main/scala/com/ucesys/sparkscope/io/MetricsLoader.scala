package com.ucesys.sparkscope.io

import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparkscope.SparkScopeConf

trait MetricsLoader {
    def load(ac: AppContext, sparkScopeConf: SparkScopeConf): DriverExecutorMetrics
}
