//package com.ucesys.sparkscope.io.file
//
//import com.amazonaws.services.s3.AmazonS3
//import com.amazonaws.services.s3.model.S3Object
//import com.ucesys.sparkscope.SparkScopeConfLoader.SparkPropertyMetricsConfS3Region
//import com.ucesys.sparkscope.common.SparkScopeConf
//import com.ucesys.sparkscope.io.metrics.S3Location
//
//import java.net.URI
//import scala.io.{BufferedSource, Source}
//
//class S3FileReader(s3: AmazonS3) extends FileReader {
//    def read(pathStr: String): String = {
//        val s3Location = S3Location(pathStr)
//        val s3Object: S3Object = s3.getObject(s3Location.bucketName, s3Location.path)
//        val myData: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
//        myData.getLines().mkString
//    }
//}
//
//object S3FileReader {
//    def apply(sparkScopeConf: SparkScopeConf): S3FileReader = {
//        val region = sparkScopeConf.sparkConf.getOption(SparkPropertyMetricsConfS3Region)
//        val s3: AmazonS3 = AmazonS3ClientBuilder
//          .standard
//          .withRegion(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
//          .build
//
//        new S3FileReader(s3)
//    }
//}
//
