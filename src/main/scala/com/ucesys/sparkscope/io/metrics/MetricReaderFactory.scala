package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.io.file.FSPrefixes.HadoopFSPrefixes
import com.ucesys.sparkscope.io.file.{HadoopFileReader, LocalFileReader}

class MetricReaderFactory(offline: Boolean) {

    def getMetricReader(sparkScopeConf: SparkScopeConf, appContext: SparkScopeContext)
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
