package com.ucesys.sparkscope.io

class FileReaderFactory {
    val HadoopFSPrefixes = Seq("maprfs:/", "hdfs:/", "file:/")

    val localFileReader = new LocalFileReader
    val hadoopFileReader = new HadoopFileReader

    def getFileReader(path: String): FileReader = {
        if(HadoopFSPrefixes.exists(path.startsWith)) {
            hadoopFileReader
        } else {
            localFileReader
        }
    }
}
