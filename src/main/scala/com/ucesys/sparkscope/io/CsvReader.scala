package com.ucesys.sparkscope.io

trait CsvReader {
  def read(path: String): String
}
