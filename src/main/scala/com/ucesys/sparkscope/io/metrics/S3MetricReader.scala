package com.ucesys.sparkscope.io.metrics

import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.{DeleteObjectsRequest, S3Object}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ucesys.sparkscope.SparkScopeConfLoader.SparkPropertyMetricsConfS3Region
import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.MetricType

import java.nio.file.Paths
import scala.collection.JavaConverters
import scala.io.{BufferedSource, Source};

class S3MetricReader(sparkScopeConf: SparkScopeConf,
                     appContext: SparkScopeContext,
                     s3: AmazonS3,
                     driverS3Location: S3Location,
                     executorS3Location: S3Location)
                    (implicit logger: SparkScopeLogger) extends MetricReader {
    def readDriver(metricType: MetricType): DataTable = {
        readMetric(metricType, driverS3Location, "driver")
    }

    def readExecutor(metricType: MetricType, executorId: String): DataTable = {
        readMetric(metricType, executorS3Location, executorId)
    }

    def readMetric(metricType: MetricType, s3Location: S3Location, instanceId: String): DataTable = {
        val appDir = Paths.get(s3Location.path, this.sparkScopeConf.appName.getOrElse("")).toString

        val rawDir: String = Paths.get(
            s3Location.path,
            ".tmp",
            appContext.appId,
            s"${instanceId}",
            s"${metricType.name}"
        ).toString;

        val finalPath: String = Paths.get(
            appDir,
            appContext.appId,
            s"${instanceId}",
            s"${metricType.name}.csv"
        ).toString;

        logger.info(s"Merging metric files for ${instanceId}.${metricType.name} from ${rawDir} to ${finalPath}")

        val files = s3.listObjects(s3Location.bucketName, rawDir).getObjectSummaries
        val objectKeys = JavaConverters.asScalaIteratorConverter(files.iterator()).asScala.toSeq.map(_.getKey);

        val header = s"t,${metricType.name}"

        val rows: Seq[String] = objectKeys.map { objectKey =>
            val s3Object: S3Object = s3.getObject(s3Location.bucketName, objectKey)
            val myData: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
            myData.getLines().mkString
        }

        val csvStr = (Seq(header) ++ rows).mkString("\n")

        val metricTable = DataTable.fromCsv(metricType.name, csvStr, ",").distinct("t").sortBy("t")
        try {
            s3.putObject(s3Location.bucketName, finalPath, csvStr)
        } catch {
            case ex: Exception => logger.error(s"Error while merging files for ${instanceId}.${metricType.name} from ${rawDir} to ${finalPath}", ex)
        }

        try {
            val dor = new DeleteObjectsRequest(s3Location.bucketName).withKeys(objectKeys:_*);
            s3.deleteObjects(dor);
        } catch {
            case ex: AmazonServiceException  =>  logger.error(s"Error while deleting tmp files from ${rawDir}", ex)
        }

        metricTable
    }
}

object S3MetricReader {
    def apply(sparkScopeConf: SparkScopeConf, appContext: SparkScopeContext)(implicit logger: SparkScopeLogger) : S3MetricReader = {
        val region = sparkScopeConf.sparkConf.getOption(SparkPropertyMetricsConfS3Region)
        val s3: AmazonS3 = AmazonS3ClientBuilder
          .standard
          .withRegion(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
          .build

        val driverS3Location = S3Location(sparkScopeConf.driverMetricsDir)
        val executorS3Location = S3Location(sparkScopeConf.executorMetricsDir)

        if (!s3.doesBucketExistV2(driverS3Location.bucketName)) {
            throw new IllegalArgumentException(s"bucket for driver metrics does not exist: ${driverS3Location.bucketName}")
        } else if (!s3.doesBucketExistV2(executorS3Location.bucketName)) {
            throw new IllegalArgumentException(s"bucket for driver metrics does not exist: ${executorS3Location.bucketName}")
        }

        try {
            val finishedPath = s".tmp/${appContext.appId}/FINISHED"
            s3.putObject(driverS3Location.bucketName, s"${driverS3Location.path}/${finishedPath}", "")

            if (driverS3Location.getUrl != executorS3Location.getUrl) {
                s3.putObject(driverS3Location.bucketName, s"${driverS3Location.path}/${finishedPath}", "")
            }
        } catch {
            case ex: AmazonServiceException  =>  logger.error(s"Error while creating FINISHED file", ex)
        }

        new S3MetricReader(
            sparkScopeConf,
            appContext,
            s3,
            driverS3Location,
            executorS3Location
        )
    }
}
