package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.metrics.SparkScopeResult

trait ReportGenerator {
    def generate(result: SparkScopeResult, sparklensResults: Seq[String]): Unit
}
