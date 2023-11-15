package com.ucesys.sparkscope.io.file

import com.ucesys.sparkscope.common.SparkScopeLogger

class FileReaderFactory(region: Option[String] = None) {
    val HadoopFSPrefixes = Seq("maprfs:/", "hdfs:/", "file:/")

    def getFileReader(path: String)(implicit logger: SparkScopeLogger) : FileReader = {
        if (HadoopFSPrefixes.exists(path.startsWith)) {
            new HadoopFileReader
        } else if (path.startsWith("s3:")) {
            S3FileReader(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
        } else {
            new LocalFileReader
        }
    }
}
