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
