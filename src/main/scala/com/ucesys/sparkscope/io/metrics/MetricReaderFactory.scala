package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.io.FSPrefixes.HadoopFSPrefixes
import com.ucesys.sparkscope.io.reader.{HadoopFileReader, LocalFileReader}

class MetricReaderFactory(offline: Boolean) {

    def getMetricReader(sparkScopeConf: SparkScopeConf, appContext: AppContext)
                       (implicit logger: SparkScopeLogger): MetricReader = {
        if (HadoopFSPrefixes.exists(sparkScopeConf.driverMetricsDir.startsWith)) {
            new HadoopMetricReader(sparkScopeConf, new HadoopFileReader, appContext)
        }
        else if (sparkScopeConf.driverMetricsDir.startsWith("s3")) {
            if (offline) {
                S3OfflineMetricReader(sparkScopeConf, appContext)
            } else {
                S3CleanupMetricReader(sparkScopeConf, appContext)
            }
        }
        else {
            new LocalMetricReader(sparkScopeConf, new LocalFileReader, appContext)
        }
    }
}
