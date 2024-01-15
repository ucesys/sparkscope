package com.ucesys.sparkscope.common

import java.io.{PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.mutable

class SparkScopeLogger(var level: LogLevel = LogLevel.Info) {
    val log = new mutable.StringBuilder()
    val timeFormat = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")

    override def toString: String = this.log.toString

    def error(str: Any, ex: Throwable, callerClass: Class[_]): Unit = {
        val sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        log(s"${str}\n${sw.toString}", LogLevel.Error, callerClass)
    }

    def error(str: Any, callerClass: Class[_]): Unit = log(str, LogLevel.Error, callerClass)
    def warn(str: Any, callerClass: Class[_]): Unit = log(str, LogLevel.Warn, callerClass)
    def info(str: Any, callerClass: Class[_]): Unit = log(str, LogLevel.Info, callerClass)
    def debug(str: Any, callerClass: Class[_]): Unit = log(str, LogLevel.Debug, callerClass)

    def log(str: Any, level: LogLevel, callerClass: Class[_]): Unit = {
        if(level >= this.level) {
            log.append(logStr(str, level, callerClass)).append("\n")
            Predef.println(logStr(str, level, callerClass))
        }
    }

    def logStr(str: Any, level: LogLevel, callerClass: Class[_]): String = {
        s"${timeFormat.format(Calendar.getInstance.getTime)} ${level} [${callerClass.getName}] ${str}"
    }
}
