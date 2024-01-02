package com.ucesys.sparkscope.io.file

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.metrics.S3Location

class S3FileWriter(s3: AmazonS3)(implicit logger: SparkScopeLogger)  extends TextFileWriter {
    def write(url: String, content: String): Unit = {
        val s3Location = S3Location(url)
        try {
            s3.putObject(s3Location.bucketName, s3Location.path, content)
        } catch {
            case ex: Exception =>
                logger.error(s"Error while writing file to ${url}", ex, this.getClass)
                throw ex
        }
    }

    def exists(url: String): Boolean = {
        val s3Location = S3Location(url)
        if (!s3.doesBucketExistV2(s3Location.bucketName)) {
            throw new IllegalArgumentException(s"${s3Location.bucketName} bucket does not exist, provided s3 url: ${url}")
        }
        s3.doesObjectExist(s3Location.bucketName, s3Location.path)
    }
}

object S3FileWriter {
    def apply(region: String)(implicit logger: SparkScopeLogger) : S3FileWriter = {
        logger.info(s"Creating s3 client for ${region} region", this.getClass)
        val s3: AmazonS3 = AmazonS3ClientBuilder
          .standard
          .withRegion(region)
          .build

        new S3FileWriter(s3)
    }
}
