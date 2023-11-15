package com.ucesys.sparkscope.io.file

trait TextFileWriter {
    def write(path: String, content: String): Unit
}
