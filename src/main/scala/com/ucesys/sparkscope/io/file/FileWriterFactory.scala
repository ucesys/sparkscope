package com.ucesys.sparkscope.io.file

import com.ucesys.sparkscope.common.SparkScopeLogger

class FileWriterFactory(region: Option[String] = None) {
    def get(path: String)(implicit logger: SparkScopeLogger): TextFileWriter = {
        if (path.startsWith("s3:")) {
            S3FileWriter(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
        } else {
            new LocalFileWriter
        }
    }
}
