package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext}
import com.ucesys.sparkscope.io.file.{HadoopFileReader, LocalFileReader}

class MetricReaderFactory {
    val HadoopFSPrefixes = Seq("maprfs:/", "hdfs:/", "file:/")

    def getMetricReader(sparkScopeConf: SparkScopeConf, appContext: SparkScopeContext): MetricReader = {
        if (HadoopFSPrefixes.exists(sparkScopeConf.driverMetricsDir.startsWith)) {
            new HadoopMetricReader(sparkScopeConf, new HadoopFileReader, appContext)
        }
//        else if (sparkScopeConf.driverMetricsDir.startsWith("s3:")) {
//            new S3CsvFileReader(sparkScopeConf)
//        }
        else {
            new LocalMetricReader(sparkScopeConf, new LocalFileReader, appContext)
        }
    }
}
