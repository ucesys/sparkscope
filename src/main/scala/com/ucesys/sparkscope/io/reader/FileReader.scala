package com.ucesys.sparkscope.io.reader

trait FileReader {
    def read(pathStr: String): String
}
