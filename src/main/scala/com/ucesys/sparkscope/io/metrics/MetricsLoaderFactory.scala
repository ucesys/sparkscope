package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext, SparkScopeLogger}

class MetricsLoaderFactory(metricReaderFactory: MetricReaderFactory)(implicit logger: SparkScopeLogger) {
    def get(sparkScopeConf: SparkScopeConf, appContext: SparkScopeContext): MetricsLoader = {
        new CsvMetricsLoader(metricReaderFactory.getMetricReader(sparkScopeConf, appContext))
    }
}
