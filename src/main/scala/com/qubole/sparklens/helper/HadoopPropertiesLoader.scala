package com.qubole.sparklens.helper

import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStreamReader
import java.net.URI
import java.util.Properties

class HadoopPropertiesLoader extends PropertiesLoader {
  def load(propertiesPath: String): Properties = {
    val fs = FileSystem.get(new URI(propertiesPath), HDFSConfigHelper.getHadoopConf(None))
    val path = new Path(propertiesPath)
    val fis = new InputStreamReader(fs.open(path))
    val prop = new Properties()
    prop.load(fis)
    prop
  }
}
