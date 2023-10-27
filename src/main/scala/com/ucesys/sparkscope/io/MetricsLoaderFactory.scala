package com.ucesys.sparkscope.io

import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparkscope.SparkScopeConf
import com.ucesys.sparkscope.utils.SparkScopeLogger


class MetricsLoaderFactory(implicit logger: SparkScopeLogger) {
    def get(sparkScopeConf: SparkScopeConf): MetricsLoader = {
        new CsvHadoopMetricsLoader(new FileReaderFactory)
    }
}
