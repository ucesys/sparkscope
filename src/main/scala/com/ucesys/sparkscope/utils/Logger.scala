package com.ucesys.sparkscope.utils

import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable

class Logger {
  val log = new mutable.StringBuilder()
  val timeFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")
  def println(x: Any): StringBuilder = {
    log.append(x).append("\n")
  }
  override def toString: String = this.log.toString
  def error(str: String): Unit = log(str, "ERROR")
  def warn(str: String): Unit = log(str, "WARN")
  def info(str: String): Unit = log(str, "INFO")
  def log(str: String, level: String): Unit = Predef.println(logStr(str, level))
  def logStr(str: Any, level: String): String = s"${timeFormat.format(Calendar.getInstance.getTime)} ${level} [SparkScope] ${str}"
}
