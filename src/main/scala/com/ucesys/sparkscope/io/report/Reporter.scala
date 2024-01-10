package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.metrics.SparkScopeResult

trait Reporter {
    def report(result: SparkScopeResult): Unit
}
