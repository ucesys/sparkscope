package com.ucesys.sparkscope.io.metrics

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ucesys.sparkscope.common.{SparkScopeConf, AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.metrics.S3CleanupMetricReader.Delimeter

import java.nio.file.Paths
import scala.collection.JavaConverters
import scala.io.{BufferedSource, Source}

class S3CleanupMetricReader(sparkScopeConf: SparkScopeConf,
                            appContext: AppContext,
                            s3: AmazonS3,
                            driverS3Location: S3Location,
                            executorS3Location: S3Location)
                           (implicit logger: SparkScopeLogger) extends MetricReader {
    def readDriver: DataTable = {
        readTmpWithCleanup(driverS3Location, "driver")
    }

    def readExecutor(executorId: String): DataTable = {
        readTmpWithCleanup(executorS3Location, executorId)
    }

    def readTmpWithCleanup(s3Location: S3Location, instanceId: String): DataTable = {
        val metricTable: DataTable = readTmpMetrics(s3Location, instanceId)

        writeMerged(s3Location, instanceId, metricTable)

        deleteTmpMetrics(s3Location, instanceId)

        metricTable
    }

    private def readTmpMetrics(s3Location: S3Location, instanceId: String): DataTable = {
        val objectKeys = listTmpMetrics(s3Location, instanceId)

        val metrics: Seq[DataTable] = objectKeys.map { objectKey =>
            logger.info(s"Reading ${objectKey} file")
            val s3Object: S3Object = s3.getObject(s3Location.bucketName, objectKey)
            val myData: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
            val csvStr = myData.getLines().mkString("\n")
            val metrics = DataTable.fromCsv(instanceId, csvStr, ",")
            logger.debug("\n" + metrics.toString)
            metrics
        }

        val header = metrics.map(_.header).toSet.size match {
            case 1 => metrics.head.header
            case 0 => throw new IllegalArgumentException(s"Couldn't read header: ${metrics.map(_.header).toSet}")
            case _ => throw new IllegalArgumentException(s"Inconsistent headers: ${metrics.map(_.header).toSet}")
        }

        val csvStr = (Seq(header) ++ metrics.map(_.toCsvNoHeader(Delimeter))).mkString("\n")
        DataTable.fromCsv(instanceId, csvStr, Delimeter).distinct("t").sortBy("t")
    }

    private def listTmpMetrics(s3Location: S3Location, instanceId: String): Seq[String] = {
        val sparseDir: String = Paths.get(
            s3Location.path,
            ".tmp",
            appContext.appId,
            s"${instanceId}"
        ).toString;
        logger.info(s"Listing instance=${instanceId} metric files from ${sparseDir}")

        val files = s3.listObjects(s3Location.bucketName, sparseDir).getObjectSummaries
        JavaConverters.asScalaIteratorConverter(files.iterator()).asScala.toSeq.map(_.getKey)
    }

    private def writeMerged(s3Location: S3Location, instanceId: String, metricTable: DataTable): Unit = {
        val appDir = Paths.get(s3Location.path, this.sparkScopeConf.appName.getOrElse("")).toString
        val mergedPath: String = Paths.get(appDir, appContext.appId, s"${instanceId}.csv").toString;
        logger.info(s"Saving merged instance=${instanceId} metrics to ${mergedPath}")

        try {
            s3.putObject(s3Location.bucketName, mergedPath, metricTable.toCsv(Delimeter))
        } catch {
            case ex: Exception => logger.error(s"Error while saving merged ${instanceId} metric for file to ${mergedPath}", ex)
        }
    }

    private def deleteTmpMetrics(s3Location: S3Location, instanceId: String): Unit = {
        val objectKeys = listTmpMetrics(s3Location, instanceId)
        try {
            val dor = new DeleteObjectsRequest(s3Location.bucketName).withKeys(objectKeys: _*);
            s3.deleteObjects(dor);
        } catch {
            case ex: AmazonServiceException => logger.error(s"Error while deleting tmp files", ex)
        }
    }
}

object S3CleanupMetricReader {
    val Delimeter = ","

    def apply(sparkScopeConf: SparkScopeConf, appContext: AppContext)(implicit logger: SparkScopeLogger) : S3CleanupMetricReader = {
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
