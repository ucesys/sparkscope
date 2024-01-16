/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

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
