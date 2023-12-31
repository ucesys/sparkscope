package com.ucesys.sparkscope.common

import java.io.{PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable

class SparkScopeLogger(var level: LogLevel = LogLevel.Info) {
    val log = new mutable.StringBuilder()
    val timeFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")

    override def toString: String = this.log.toString

    def error(str: Any, ex: Throwable): Unit = {
        val sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        log(s"${str}\n${sw.toString}", LogLevel.Error)
    }

    def error(str: Any): Unit = log(str, LogLevel.Error)
    def warn(str: Any): Unit = log(str, LogLevel.Warn)
    def info(str: Any): Unit = log(str, LogLevel.Info)
    def debug(str: Any): Unit = log(str, LogLevel.Debug)

    def log(str: Any, level: LogLevel): Unit = {
        if(level >= this.level) {
            log.append(logStr(str, level)).append("\n")
            Predef.println(logStr(str, level))
        }
    }

    def logStr(str: Any, level: LogLevel): String = s"${timeFormat.format(Calendar.getInstance.getTime)} ${level} [SparkScope] ${str}"
}
