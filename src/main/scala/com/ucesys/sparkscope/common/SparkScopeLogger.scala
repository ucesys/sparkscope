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

    def error(str: Any, callerClass: Class[_], stdout: Boolean = true): Unit = log(str, LogLevel.Error, callerClass, stdout)
    def warn(str: Any, callerClass: Class[_], stdout: Boolean = true): Unit = log(str, LogLevel.Warn, callerClass, stdout)
    def info(str: Any, callerClass: Class[_], stdout: Boolean = true): Unit = log(str, LogLevel.Info, callerClass, stdout)
    def debug(str: Any, callerClass: Class[_], stdout: Boolean = true): Unit = log(str, LogLevel.Debug, callerClass, stdout)

    def log(str: Any, level: LogLevel, callerClass: Class[_], stdout: Boolean = true): Unit = {
        if(level >= this.level) {
            if (stdout) {
                Predef.println(logStr(str, level, callerClass))
            }
            log.append(logStr(str, level, callerClass)).append("\n")
        }
    }

    def logStr(str: Any, level: LogLevel, callerClass: Class[_]): String = {
        s"${timeFormat.format(Calendar.getInstance.getTime)} ${level} [${callerClass.getName}] ${str}"
    }
}
