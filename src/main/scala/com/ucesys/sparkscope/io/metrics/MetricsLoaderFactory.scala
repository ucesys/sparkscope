package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext, SparkScopeLogger}

class MetricsLoaderFactory(implicit logger: SparkScopeLogger) {
    def get(sparkScopeConf: SparkScopeConf, appContext: SparkScopeContext): MetricsLoader = {
        new CsvMetricsLoader((new MetricReaderFactory).getMetricReader(sparkScopeConf, appContext))
    }
}
