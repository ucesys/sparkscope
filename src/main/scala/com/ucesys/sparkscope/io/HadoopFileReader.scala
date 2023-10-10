package com.ucesys.sparkscope.io

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

class HadoopFileReader extends FileReader {
  def read(pathStr: String): String = {
      val fs = FileSystem.get(new URI(pathStr), new Configuration())
      val path = new Path(pathStr)
      val byteArray = new Array[Byte](fs.getFileStatus(path).getLen.toInt)
      fs.open(path).readFully(byteArray)
      (byteArray.map(_.toChar)).mkString
  }
}
