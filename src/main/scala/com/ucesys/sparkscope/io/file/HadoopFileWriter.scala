package com.ucesys.sparkscope.io.file

import com.ucesys.sparkscope.common.SparkScopeLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io
import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets.UTF_8;

class HadoopFileWriter(conf: Configuration)(implicit logger: SparkScopeLogger) extends TextFileWriter {

    val fs: FileSystem = try {
        FileSystem.get(conf)
    } catch {
        case e: IOException =>
            logger.error(s"IOException while creating hadoop filesystem. ${e}", e)
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
                logger.error(s"IOException while writing ${content} to ${pathStr}. ${e}")
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