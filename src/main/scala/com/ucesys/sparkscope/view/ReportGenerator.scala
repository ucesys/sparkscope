package com.ucesys.sparkscope.view

import com.ucesys.sparkscope.metrics.SparkScopeResult

trait ReportGenerator {
    def generate(result: SparkScopeResult, sparklensResults: Seq[String]): Unit
}
