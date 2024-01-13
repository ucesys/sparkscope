package com.ucesys.sparkscope.io.writer

trait TextFileWriter {
    def write(destination: String, content: String): Unit

    def exists(path: String): Boolean
}
