package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.data.DataFrame

case class DriverExecutorMetrics(driverMetrics: Seq[DataFrame],
                                 executorMetricsMap: Map[Int, Seq[DataFrame]])
