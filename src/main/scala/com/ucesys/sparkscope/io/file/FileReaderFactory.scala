package com.ucesys.sparkscope.io.file

import com.ucesys.sparkscope.common.SparkScopeConf

class FileReaderFactory {
    val HadoopFSPrefixes = Seq("maprfs:/", "hdfs:/", "file:/")

    def getFileReader(path: String): FileReader = {
        if (HadoopFSPrefixes.exists(path.startsWith)) {
            new HadoopFileReader
        }
//        else if (sparkScopeConf.driverMetricsDir.startsWith("s3:")) {
//            new S3CsvFileReader(sparkScopeConf)
//        }
        else {
            new LocalFileReader
        }
    }
}
