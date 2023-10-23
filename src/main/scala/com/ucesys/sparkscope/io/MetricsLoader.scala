package com.ucesys.sparkscope.io

trait MetricsLoader {
    def load(): DriverExecutorMetrics
}
