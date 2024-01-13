package com.ucesys.sparkscope.io.file

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.file.FSPrefixes.{HadoopFSPrefixes, S3Prefix}

class FileReaderFactory(region: Option[String] = None) {
    def getFileReader(path: String)(implicit logger: SparkScopeLogger) : FileReader = {
        if (HadoopFSPrefixes.exists(path.startsWith)) {
            new HadoopFileReader
        } else if (path.startsWith(S3Prefix)) {
            S3FileReader(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
        } else {
            new LocalFileReader
        }
    }
}
