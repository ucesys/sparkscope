# Changelog

## [0.1.2] - 2023-11-08
- Add custom SparkScopeCsvSink
  - hdfs:/ and maprfs:/ directories are treated as hadoop directories and are handled by HdfsCsvReporter
  - file:/ and other directories are treated as local and are handled by LocalCsvReporter
  - sink now dumps only useful metrics:
    - 5 executor metrics: jvm.heap.used, jvm.heap.usage, jvm.heap.max, jvm.non-heap.used, executor.cpuTime
    - 4 driver metrics: jvm.heap.used, jvm.heap.usage, jvm.heap.max, jvm.non-heap.used
  - fixed calculation of wasted CPU/Memory

## [0.1.1] - 2023-10-30
- Create SparkScopeApp with CLI to run SparkScope as standalone app 
  - reads application context, spark conf and events from eventLog  
  - can be run on finished or running spark application

## [0.1.0] - 2023-10-10
Initial SparkScope-spark3 release:
- Compatible:
  - JDK: 8/11/17
  - Spark: 3.2/3.3/3.4/3.5
- Charts:
  - heap & non-heap usage charts
  - cpu utilization charts
  - charts for driver, executors and aggregated charts for whole application
- Stats:
  - heap & non-heap usage stats
  - cpu utilization and memory utlization stats
  - stats for driver, executors and aggregated stats for whole application
  - CPU and Heap Memory Waste stats
