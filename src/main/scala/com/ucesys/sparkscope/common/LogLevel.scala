package com.ucesys.sparkscope.common

case class LogLevel(str: String, level: Int) {
    override def toString: String = str
    def >=(other: LogLevel): Boolean = this.level >= other.level
}

object LogLevel {
    val Error = LogLevel("ERROR", 4)
    val Warn = LogLevel("WARN", 3)
    val Info = LogLevel("INFO", 2)
    val Debug = LogLevel("DEBUG", 1)

    def fromString(levelStr: String): LogLevel = levelStr.toLowerCase match {
        case "error" => Error
        case "warn" => Warn
        case "info" => Info
        case "debug" => Debug
        case _ => throw new IllegalArgumentException(s"Unknown log level: ${levelStr}")
    }
}
