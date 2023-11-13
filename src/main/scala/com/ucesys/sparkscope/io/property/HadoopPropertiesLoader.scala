package com.ucesys.sparkscope.io.property

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStreamReader
import java.net.URI
import java.util.Properties

class HadoopPropertiesLoader(propertiesPathStr: String) extends PropertiesLoader {
    def load(): Properties = {
        val fs = FileSystem.get(new URI(propertiesPathStr), new Configuration())
        val path = new Path(propertiesPathStr)
        val fis = new InputStreamReader(fs.open(path))
        val prop = new Properties()
        prop.load(fis)
        prop
    }
}
