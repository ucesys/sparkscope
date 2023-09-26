package com.ucesys.sparkscope.io

import java.nio.file.{Files, Paths}

class LocalFileReader extends FileReader {
  def read(pathStr: String): String = new String(Files.readAllBytes(Paths.get(pathStr)))
}
