package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.ExecutorMetricsAnalyzer.{CpuTime, NanoSecondsInSec}
import com.ucesys.sparkscope.data.DataFrame

case class ClusterCPUMetrics(clusterCpuTime: DataFrame, clusterUsageDf: DataFrame)

object ClusterCPUMetrics {
  def apply(allExecutorsMetrics: DataFrame, executorCores: Int): ClusterCPUMetrics = {
    val clusterCpuTimeDf = allExecutorsMetrics.groupBy("t", CpuTime).sum.sortBy("t")

    val clusterCpuTime = clusterCpuTimeDf.select(CpuTime).div(NanoSecondsInSec)
    val clusterCpuTimeLag = clusterCpuTime.lag
    val clusterCpuTimeDiff = clusterCpuTime.sub(clusterCpuTimeLag)
    val timeLag = clusterCpuTimeDf.select("t").lag
    val timeElapsed = clusterCpuTimeDf.select("t").sub(timeLag)
    val numExecutors = allExecutorsMetrics.groupBy("t", CpuTime).count.sortBy("t")
    val combinedTimeElapsed = timeElapsed.mul(numExecutors.select("cnt")).mul(executorCores)
    val clusterUsage = clusterCpuTimeDiff.div(combinedTimeElapsed)

    val clusterUsageDf = DataFrame(
      "cpuUsage",
      Seq(
        clusterCpuTimeDf.select("t"),
        timeElapsed.rename("dt"),
        combinedTimeElapsed.rename("dt*executorCnt*coresPerExec"),
        clusterCpuTime,
        clusterCpuTimeDiff.rename("cpuTimeDiff"),
        numExecutors.select("cnt").rename("executorCnt"),
        clusterUsage.rename("cpuUsage")
      ))

    ClusterCPUMetrics(clusterCpuTime=clusterCpuTimeDf, clusterUsageDf=clusterUsageDf)
  }
}
