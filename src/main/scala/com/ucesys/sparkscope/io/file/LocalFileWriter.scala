package com.ucesys.sparkscope.io.file

import java.io.{File, FileOutputStream, FileWriter, OutputStreamWriter, PrintWriter}
import java.nio.charset.StandardCharsets.UTF_8

class LocalFileWriter extends TextFileWriter {
    def write(path: String, content: String): Unit = {
        val fileWriter = new FileWriter(path)
        fileWriter.write(content)
        fileWriter.close()
    }

    def append(path: String, content: String): Unit = {
        val writer: PrintWriter = new PrintWriter(new OutputStreamWriter(new FileOutputStream(new File(path), true), UTF_8))
        writer.println(content)
        writer.close()
    }

    def exists(path: String): Boolean = {
        new File(path).exists()
    }
}
