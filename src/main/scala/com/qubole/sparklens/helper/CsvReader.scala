package com.qubole.sparklens.helper

trait CsvReader {
  def read(path: String): String
}
