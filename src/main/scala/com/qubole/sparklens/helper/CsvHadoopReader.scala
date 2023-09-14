package com.qubole.sparklens.helper

import org.apache.hadoop.fs.{FileSystem, Path}

import java.net.URI

class CsvHadoopReader extends CsvReader {
  def read(pathStr: String): String = {
      println(pathStr)
      val fs = FileSystem.get(new URI(pathStr), HDFSConfigHelper.getHadoopConf(None))
      val path = new Path(pathStr)
      val byteArray = new Array[Byte](fs.getFileStatus(path).getLen.toInt)
      fs.open(path).readFully(byteArray)
      (byteArray.map(_.toChar)).mkString
  }
}
