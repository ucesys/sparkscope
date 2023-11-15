package com.ucesys.sparkscope.io.file

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.file.FileSystem.{HadoopFSPrefixes, S3Prefix}

class FileWriterFactory(region: Option[String] = None) {
    def get(path: String)(implicit logger: SparkScopeLogger): TextFileWriter = {
        if (HadoopFSPrefixes.exists(path.startsWith)) {
            new HadoopFileWriter
        } else if (path.startsWith(S3Prefix)) {
            S3FileWriter(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
        } else {
            new LocalFileWriter
        }
    }
}
