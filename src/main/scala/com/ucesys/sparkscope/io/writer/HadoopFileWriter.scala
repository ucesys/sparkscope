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

package com.ucesys.sparkscope.io.writer

import com.ucesys.sparkscope.common.SparkScopeLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets.UTF_8;

class HadoopFileWriter(conf: Configuration)(implicit logger: SparkScopeLogger) extends TextFileWriter {

    val fs: FileSystem = try {
        FileSystem.get(conf)
    } catch {
        case e: IOException =>
            logger.error(s"IOException while creating hadoop filesystem. ${e}", e, this.getClass)
            throw e
    }
    def write(pathStr: String, content: String): Unit = {
        val path = new Path(pathStr)
        val writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true), UTF_8))
        writeInternal(pathStr, content, writer)
    }

    def append(pathStr: String, content: String): Unit = {
        val path = new Path(pathStr)
        val writer = new BufferedWriter(new OutputStreamWriter(fs.append(path)))
        writeInternal(pathStr, content, writer)
    }

    def writeInternal(pathStr: String, content: String, writer: BufferedWriter): Unit = {
        try {
            writer.write(content)
        } catch {
            case e: IOException =>
                logger.error(s"IOException while writing ${content} to ${pathStr}. ${e}", this.getClass)
                throw e
        } finally {
            writer.close()
        }
    }

    def makeDir(pathStr: String): Unit = {
        fs.mkdirs(new Path(pathStr));
    }

    @throws(classOf[IOException])
    def exists(pathStr: String): Boolean = {
        fs.exists(new Path(pathStr))
    }
}