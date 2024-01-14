package com.ucesys.sparkscope.io

trait FileReader {
    def read(pathStr: String): String
}
