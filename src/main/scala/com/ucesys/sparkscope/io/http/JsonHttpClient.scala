package com.ucesys.sparkscope.io.http

import com.ucesys.sparkscope.common.SparkScopeLogger

class JsonHttpClient(implicit logger: SparkScopeLogger) {
    def post(url: String, jsonStr: String): Unit = {
        logger.info(s"Posting json to ${url}: ${jsonStr}", this.getClass)
        println(s"Posting json to ${url}: ${jsonStr}", this.getClass)
    }
}