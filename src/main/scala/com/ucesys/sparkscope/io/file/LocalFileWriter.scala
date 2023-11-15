package com.ucesys.sparkscope.io.file

import java.io.FileWriter

class LocalFileWriter extends TextFileWriter {
    def write(path: String, content: String): Unit = {
        val fileWriter = new FileWriter(path)
        fileWriter.write(content)
        fileWriter.close()
    }
}
