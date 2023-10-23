package com.ucesys.sparkscope.utils

import java.io.{PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable

class SparkScopeLogger private {
    val log = new mutable.StringBuilder()
    val timeFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")

    def println(x: Any): StringBuilder = {
        log.append(x).append("\n")
    }

    override def toString: String = this.log.toString

    def error(str: Any, ex: Throwable): Unit = {
        val sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        log(s"${str}\n${sw.toString()}", "ERROR")
    }

    def error(str: Any): Unit = log(str, "ERROR")

    def warn(str: Any): Unit = log(str, "WARN")

    def info(str: Any): Unit = log(str, "INFO")

    def log(str: Any, level: String): Unit = {
        this.println(logStr(str, level))
        Predef.println(logStr(str, level))
    }

    def logStr(str: Any, level: String): String = s"${timeFormat.format(Calendar.getInstance.getTime)} ${level} [SparkScope] ${str}"
}

object SparkScopeLogger {
    private val logger = new SparkScopeLogger

    def get: SparkScopeLogger = logger
}
