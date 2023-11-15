package com.ucesys.sparkscope.io.metrics

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.MetricType

import java.nio.file.Paths
import scala.collection.JavaConverters
import scala.io.{BufferedSource, Source}

class S3CleanupMetricReader(sparkScopeConf: SparkScopeConf,
                            appContext: SparkScopeContext,
                            s3: AmazonS3,
                            driverS3Location: S3Location,
                            executorS3Location: S3Location)
                           (implicit logger: SparkScopeLogger) extends MetricReader {
    def readDriver(metricType: MetricType): DataTable = {
        readTmpWithCleanup(metricType, driverS3Location, "driver")
    }

    def readExecutor(metricType: MetricType, executorId: String): DataTable = {
        readTmpWithCleanup(metricType, executorS3Location, executorId)
    }

    def readTmpWithCleanup(metricType: MetricType, s3Location: S3Location, instanceId: String): DataTable = {
        val metricTable: DataTable = readTmpMetrics(metricType, s3Location, instanceId)

        writeMerged(metricType, s3Location, instanceId, metricTable)

        deleteTmpMetrics(metricType, s3Location, instanceId)

        metricTable
    }

    private def readTmpMetrics(metricType: MetricType, s3Location: S3Location, instanceId: String): DataTable = {
        logger.info(s"Reading tmp ${instanceId}/${metricType.name} metric files")
        val objectKeys = listTmpMetrics(metricType, s3Location, instanceId)

        val header = s"t,${metricType.name}"
        val rows: Seq[String] = objectKeys.map { objectKey =>
            val s3Object: S3Object = s3.getObject(s3Location.bucketName, objectKey)
            val myData: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
            myData.getLines().mkString
        }

        val csvStr = (Seq(header) ++ rows).mkString("\n")
        DataTable.fromCsv(metricType.name, csvStr, ",").distinct("t").sortBy("t")
    }

    private def listTmpMetrics(metricType: MetricType, s3Location: S3Location, instanceId: String): Seq[String] = {
        val sparseDir: String = Paths.get(
            s3Location.path,
            ".tmp",
            appContext.appId,
            s"${instanceId}",
            s"${metricType.name}"
        ).toString;

        val files = s3.listObjects(s3Location.bucketName, sparseDir).getObjectSummaries
        JavaConverters.asScalaIteratorConverter(files.iterator()).asScala.toSeq.map(_.getKey)
    }

    private def writeMerged(metricType: MetricType, s3Location: S3Location, instanceId: String, metricTable: DataTable): Unit = {
        val appDir = Paths.get(s3Location.path, this.sparkScopeConf.appName.getOrElse("")).toString
        val mergedPath: String = Paths.get(appDir, appContext.appId, s"${instanceId}", s"${metricType.name}.csv").toString;
        logger.info(s"Saving merged ${instanceId}/${metricType.name} metric for file to ${mergedPath}")

        try {
            s3.putObject(s3Location.bucketName, mergedPath, metricTable.toCsv(""))
        } catch {
            case ex: Exception => logger.error(s"Error while saving merged ${instanceId}/${metricType.name} metric for file to ${mergedPath}", ex)
        }
    }

    private def deleteTmpMetrics(metricType: MetricType, s3Location: S3Location, instanceId: String): Unit = {
        val objectKeys = listTmpMetrics(metricType, s3Location, instanceId)
        try {
            val dor = new DeleteObjectsRequest(s3Location.bucketName).withKeys(objectKeys: _*);
            s3.deleteObjects(dor);
        } catch {
            case ex: AmazonServiceException => logger.error(s"Error while deleting tmp files", ex)
        }
    }
}

object S3CleanupMetricReader {
    def apply(sparkScopeConf: SparkScopeConf, appContext: SparkScopeContext)(implicit logger: SparkScopeLogger) : S3CleanupMetricReader = {
        val region = sparkScopeConf.region
        val s3: AmazonS3 = AmazonS3ClientBuilder
          .standard
          .withRegion(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
          .build

        val driverS3Location = S3Location(sparkScopeConf.driverMetricsDir)
        val executorS3Location = S3Location(sparkScopeConf.executorMetricsDir)

        val inProgressPath = s".tmp/${appContext.appId}/IN_PROGRESS"
        try {
            logger.info(s"Deleting ${inProgressPath} file")
            s3.deleteObject(driverS3Location.bucketName, s"${driverS3Location.path}/${inProgressPath}")

            if (driverS3Location.getUrl != executorS3Location.getUrl) {
                logger.info(s"Deleting ${inProgressPath} file")
                s3.deleteObject(executorS3Location.bucketName, s"${executorS3Location.path}/${inProgressPath}")
            }
        } catch {
            case ex: AmazonServiceException =>
                logger.error(s"Error while deleting ${inProgressPath}", ex)
                throw ex
        }

        new S3CleanupMetricReader(
            sparkScopeConf,
            appContext,
            s3,
            driverS3Location,
            executorS3Location
        )
    }
}
