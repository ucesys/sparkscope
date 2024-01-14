package com.ucesys.sparkscope.io

class PropertiesLoaderFactory {
    val HadoopFSPrefixes = Seq("maprfs:/", "hdfs:/", "file:/")

    def getPropertiesLoader(path: String): PropertiesLoader = {
        if(HadoopFSPrefixes.exists(path.startsWith)) {
            new HadoopPropertiesLoader(path)
        } else {
            new LocalPropertiesLoader(path)
        }
    }

}
