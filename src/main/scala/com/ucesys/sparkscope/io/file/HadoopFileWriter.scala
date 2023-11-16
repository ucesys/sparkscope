package com.ucesys.sparkscope.io.file

import com.ucesys.sparkscope.common.SparkScopeLogger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedWriter, IOException, OutputStreamWriter}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8;

class HadoopFileWriter(implicit logger: SparkScopeLogger)  extends TextFileWriter {
    def write(pathStr: String, content: String): Unit = {
        val fs = FileSystem.get(new URI(pathStr), new Configuration)
        val path = new Path(pathStr)
        val writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true), UTF_8))
        try {
            writer.write(content);
        } catch {
            case e: IOException => logger.warn("IOException while creating csv file: {}", path.getName, e);
        } finally {
            writer.close()
            fs.close()
        }
    }
}