package com.ucesys.sparkscope.report

import com.ucesys.sparkscope.metrics.SparkScopeResult

trait Reporter {
    def report(result: SparkScopeResult): Unit
}
