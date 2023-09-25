package com.ucesys.sparkscope.io

import com.ucesys.sparklens.helper.HDFSConfigHelper
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.InputStreamReader
import java.net.URI
import java.util.Properties

class HadoopPropertiesLoader(propertiesPathStr: String) extends PropertiesLoader {
  def load(): Properties = {
    val fs = FileSystem.get(new URI(propertiesPathStr), HDFSConfigHelper.getHadoopConf(None))
    val path = new Path(propertiesPathStr)
    val fis = new InputStreamReader(fs.open(path))
    val prop = new Properties()
    prop.load(fis)
    prop
  }
}
