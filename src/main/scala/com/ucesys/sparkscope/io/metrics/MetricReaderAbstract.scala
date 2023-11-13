package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.SparkScopeConf
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.{InstanceType, MetricType}

abstract class MetricReaderAbstract(driverMetricsDir: String, executorMetricsDir: String) {
    def this(sparkScopeConf: SparkScopeConf) {
        this(sparkScopeConf.driverMetricsDir, sparkScopeConf.executorMetricsDir)
    }
    def read(instanceType: InstanceType, metricType: MetricType): DataTable
}

//object MetricReader {
//    def apply(sparkScopeConf: SparkScopeConf): MetricReader = {
//        new MetricReader(sparkScopeConf.driverMetricsDir, sparkScopeConf.executorMetricsDir)
//    }
//}
