package com.ucesys.sparkscope.io.writer

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.FSPrefixes.{HadoopFSPrefixes, S3Prefix}
import org.apache.hadoop.conf.Configuration

class FileWriterFactory {
    def get(path: String, region: Option[String] = None)(implicit logger: SparkScopeLogger): TextFileWriter = {
        if (HadoopFSPrefixes.exists(path.startsWith)) {
            new HadoopFileWriter(new Configuration)
        } else if (path.startsWith(S3Prefix)) {
            S3FileWriter(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
        } else {
            new LocalFileWriter
        }
    }
}
