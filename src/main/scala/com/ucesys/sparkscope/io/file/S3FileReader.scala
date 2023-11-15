package com.ucesys.sparkscope.io.file

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.S3Object
import com.ucesys.sparkscope.io.metrics.S3Location

import scala.io.{BufferedSource, Source}

class S3FileReader(s3: AmazonS3) extends FileReader {
    def read(url: String): String = {
        val s3Location = S3Location(url)
        println(s"Reading url: ${url}, bucket: ${s3Location.bucketName}, key: ${s3Location.path}")
        val s3Object: S3Object = s3.getObject(s3Location.bucketName, s3Location.path)
        val eventLogSource: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
        eventLogSource.getLines().mkString("\n")
    }
}

object S3FileReader {
    def apply(region: String): S3FileReader = {
        println(s"Creating s3 client for ${region} region")
        val s3: AmazonS3 = AmazonS3ClientBuilder
          .standard
          .withRegion(region)
          .build

        new S3FileReader(s3)
    }
}

