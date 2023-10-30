package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeLogger}

class MetricsLoaderFactory(implicit logger: SparkScopeLogger) {
    def get(sparkScopeConf: SparkScopeConf): MetricsLoader = {
        new CsvHadoopMetricsLoader(new FileReaderFactory)
    }
}
