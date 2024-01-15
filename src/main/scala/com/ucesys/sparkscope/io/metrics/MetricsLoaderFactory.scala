package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, AppContext, SparkScopeLogger}

class MetricsLoaderFactory(metricReaderFactory: MetricReaderFactory)(implicit logger: SparkScopeLogger) {
    def get(sparkScopeConf: SparkScopeConf, appContext: AppContext): MetricsLoader = {
        new CsvMetricsLoader(metricReaderFactory.getMetricReader(sparkScopeConf, appContext))
    }
}
