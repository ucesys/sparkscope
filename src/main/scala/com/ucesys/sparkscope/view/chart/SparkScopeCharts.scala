package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.MemorySize.BytesInMB
import com.ucesys.sparkscope.common.{JvmHeapMax, JvmHeapUsage, JvmHeapUsed, JvmNonHeapUsed, SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.common.MetricUtils.{ColCpuUsage, ColTs}
import com.ucesys.sparkscope.metrics.SparkScopeMetrics

case class SparkScopeCharts(cpuUtilChart: SimpleChart,
                            heapUtilChart: SimpleChart,
                            cpuUtilsVsCapacityChart: LimitedChart,
                            heapUtilVsSizeChart: LimitedChart,
                            tasksChart: LimitedChart,
                            numExecutorsChart: SimpleChart,
                            executorHeapChart: ExecutorChart,
                            executorNonHeapChart: ExecutorChart,
                            driverHeapUtilChart: LimitedChart,
                            driverNonHeapUtilChart: LimitedChart)

object SparkScopeCharts {
    def fromMetrics(sparkScopeConf: SparkScopeConf, metrics: SparkScopeMetrics)
                   (implicit logger: SparkScopeLogger) : SparkScopeCharts = {
        val cpuUtilChart = SimpleChart(
            metrics.clusterCpu.clusterCpuUsage.select(ColTs),
            metrics.clusterCpu.clusterCpuUsage.select(ColCpuUsage).mul(100)
        )

        val heapUtilChart = SimpleChart(
            metrics.clusterMemory.heapUsage.select(ColTs),
            metrics.clusterMemory.heapUsage.select(JvmHeapUsage.name).mul(100)
        )

        val cpuUtilsVsCapacityChart = LimitedChart(
            metrics.clusterCpu.clusterCpuUsage.select(ColTs),
            metrics.clusterCpu.clusterCpuUsageSum.select("cpuUsageAllCores"),
            metrics.clusterCpu.clusterCapacity.select("totalCores")
        )

        val heapUtilVsSizeChart = LimitedChart(
            metrics.clusterMemory.heapUsed.select(ColTs),
            metrics.clusterMemory.heapUsed.select(JvmHeapUsed.name).div(BytesInMB),
            metrics.clusterMemory.heapMax.select(JvmHeapMax.name).div(BytesInMB)
        )

        val tasksChart = LimitedChart(
            metrics.clusterCpu.clusterCapacity.select(ColTs),
            metrics.stage.numberOfTasks,
            metrics.clusterCpu.clusterCapacity.select("totalCores")
        )

        val numExecutorsChart = SimpleChart(
            metrics.clusterCpu.numExecutors.select(ColTs),
            metrics.clusterCpu.numExecutors.select("cnt")
        )

        val executorHeapChart = ExecutorChart(
            metrics.executor.heapUsedMax.select(ColTs),
            metrics.executor.heapAllocation.select(JvmHeapMax.name).div(BytesInMB),
            metrics.executor.executorMetricsMap.map { case (id, metrics) => metrics.select(JvmHeapUsed.name).div(BytesInMB).rename(id) }.toSeq
        )

        val executorNonHeapChart = ExecutorChart(
            metrics.executor.nonHeapUsedMax.select(ColTs),
            metrics.executor.nonHeapUsedMax.addConstColumn("memoryOverhead", sparkScopeConf.executorMemOverhead.toMB.toString).select("memoryOverhead"),
            metrics.executor.executorMetricsMap.map { case (id, metrics) => metrics.select(JvmNonHeapUsed.name).div(BytesInMB).rename(id) }.toSeq
        )

        val driverHeapUtilChart = LimitedChart(
            metrics.driver.select(ColTs),
            metrics.driver.select(JvmHeapUsed.name).div(BytesInMB),
            metrics.driver.select(JvmHeapMax.name).div(BytesInMB)
        )

        val driverNonHeapUtilChart = LimitedChart(
            metrics.driver.select(ColTs),
            metrics.driver.select("jvm.non-heap.used").div(BytesInMB),
            metrics.driver.addConstColumn("memoryOverhead", sparkScopeConf.driverMemOverhead.toMB.toString).select("memoryOverhead")
        )

        SparkScopeCharts(
            cpuUtilChart = cpuUtilChart,
            heapUtilChart = heapUtilChart,
            cpuUtilsVsCapacityChart = cpuUtilsVsCapacityChart,
            heapUtilVsSizeChart = heapUtilVsSizeChart,
            tasksChart = tasksChart,
            numExecutorsChart = numExecutorsChart,
            executorHeapChart = executorHeapChart,
            executorNonHeapChart = executorNonHeapChart,
            driverHeapUtilChart = driverHeapUtilChart,
            driverNonHeapUtilChart = driverNonHeapUtilChart
        )
    }
}
