package com.ucesys.sparkscope.io.metrics

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ucesys.sparkscope.common.{MetricType, SparkScopeConf, AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.file.S3FileReader

import java.nio.file.Paths

class S3OfflineMetricReader(sparkScopeConf: SparkScopeConf,
                            appContext: AppContext,
                            driverS3Location: S3Location,
                            executorS3Location: S3Location,
                            reader: S3FileReader)
                           (implicit logger: SparkScopeLogger) extends MetricReader {
    def readDriver: DataTable = {
        readMerged(driverS3Location, "driver")
    }

    def readExecutor(executorId: String): DataTable = {
        readMerged(executorS3Location, executorId)
    }

    private def readMerged(s3Location: S3Location, instanceId: String): DataTable = {
        val appDir = Paths.get(s3Location.path, this.sparkScopeConf.appName.getOrElse("")).toString
        val mergedPath: String = Paths.get(appDir, appContext.appId, s"${instanceId}.csv").toString
        logger.info(s"Reading merged ${instanceId} metric file from ${mergedPath}")

        val csvStr = reader.read(S3Location(s3Location.bucketName, mergedPath).getUrl)
        DataTable.fromCsv(instanceId, csvStr, ",").distinct("t").sortBy("t")
    }
}

object S3OfflineMetricReader {
    def apply(sparkScopeConf: SparkScopeConf, appContext: AppContext)(implicit logger: SparkScopeLogger) : S3OfflineMetricReader = {
        val region = sparkScopeConf.region
        val s3: AmazonS3 = AmazonS3ClientBuilder
          .standard
          .withRegion(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
          .build

        val driverS3Location = S3Location(sparkScopeConf.driverMetricsDir)
        val executorS3Location = S3Location(sparkScopeConf.executorMetricsDir)

        new S3OfflineMetricReader(
            sparkScopeConf,
            appContext,
            driverS3Location,
            executorS3Location,
            new S3FileReader(s3)
        )
    }
}
