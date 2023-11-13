package com.ucesys.sparkscope.io.file

trait FileReader {
    def read(pathStr: String): String
}
