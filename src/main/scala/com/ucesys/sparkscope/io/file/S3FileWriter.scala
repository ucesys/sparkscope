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
                logger.error(s"Error while writing file to ${url}", ex)
                throw ex
        }
    }
}

object S3FileWriter {
    def apply(region: String)(implicit logger: SparkScopeLogger) : S3FileWriter = {
        println(s"Creating s3 client for ${region} region")
        val s3: AmazonS3 = AmazonS3ClientBuilder
          .standard
          .withRegion(region)
          .build

        new S3FileWriter(s3)
    }
}
