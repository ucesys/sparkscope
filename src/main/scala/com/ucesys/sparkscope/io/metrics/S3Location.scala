package com.ucesys.sparkscope.io.metrics

case class S3Location(bucketName: String, path: String) {
    def getUrl: String = s"s3://${bucketName}/${path}"
}

object S3Location {
    def apply(s3Path: String): S3Location = s3Path.split("/").toSeq.filter(_.nonEmpty) match {
        case Seq(fs, bucketName, metricsDirTail @_*) if metricsDirTail.nonEmpty => S3Location(bucketName, metricsDirTail.mkString("/"))
        case _ => throw new IllegalArgumentException(s"Couldn't parse s3 directory path: ${s3Path}")
    }
}