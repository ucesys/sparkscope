package com.ucesys.sparkscope.utils

import scala.collection.mutable

class Logger {
  val log = new mutable.StringBuilder()

  def println(x: Any): StringBuilder = {
    log.append(x).append("\n")
  }
  override def toString: String = this.log.toString
}
