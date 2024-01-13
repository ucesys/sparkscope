# Changelog

## [0.1.9] - 2024-01-10 Resource allocation and Json reporter
- Added Core-hours and Heap-hourse allocation info to report 
- Added new JSON report format that can be used apart from HTML
- Added sending diagnostics data

## [0.1.8] - 2024-01-02 New Warnings and Logging enhancements 
- New warnings added:
  - Data Spill from memory to disk warning
  - Long time spent in Garbage Collection warning
- Logging enhancements:
  - Logging level can now be set to DEBUG/INFO/WARN/ERROR
  - Log file can now be written to a different directory than html report
  - Log file can now be written to a different filesystem than html report
  - Full package and class name added to log format

## [0.1.7] - 2023-12-06 UI Enhancements
- Replace stage chart with number of tasks vs CPU capacity chart
- Remove SparkScope logs from HTML report
- SparkScope logs are now contained in separate .log file

## [0.1.6] - 2023-12-04 UI responsiveness optimization
- Charts data points limitation:
  - All charts have maximum number of rendered datapoints
  - When number of datapoints exceeds the limit, chart values are interpolated 
- EventLog prefiltering:
  - EventLog context loader prefilters events before parsing them to json and applying filtering

## [0.1.5] - 2023-11-29 Metrics spilling optimization
- Metrics spill refactor
  - we are now spilling single merged csv file per instance containing all metrics instead of 5 separate files(heap used, heap used in %, heap size, non-heap used, cpuTime)
  - local&hadoop metrics are grouped by appName+appId on storage(just like s3 metrics)
- All sinks/reporters rewritten from Java to Scala

## [0.1.4] - 2023-11-23 UI Enhancements
- New charts
  - chart of stages in time with number of tasks per stage
  - chart of active executors in time
  - seperate memory charts for each executor instead of aggregated
- UI enhancements
  - appName field added to application summary
  - added executor and driver memoryOverheads to charts

## [0.1.3] - 2023-11-16 S3 compatiblity
- S3 compatibility
  - spilling metrics to s3 (SparkScopeCsvSink will spill metrics to s3 for directory starting with s3/s3a/s3n prefix)
  - analyzing metrics spilled to s3 (SparkScopeListener can read metrics from s3)
  - running offline on eventlog stored in s3 (SparkScopeApp can run offline on eventlog stored in s3)
  - saving html reports to s3

## [0.1.2] - 2023-11-08 Hadoop compatiblity
- Add custom SparkScopeCsvSink
  - hdfs:/ and maprfs:/ directories are treated as hadoop directories and are handled by HdfsCsvReporter
  - file:/ and other directories are treated as local and are handled by LocalCsvReporter
  - sink now dumps only useful metrics:
    - 5 executor metrics: jvm.heap.used, jvm.heap.usage, jvm.heap.max, jvm.non-heap.used, executor.cpuTime
    - 4 driver metrics: jvm.heap.used, jvm.heap.usage, jvm.heap.max, jvm.non-heap.used
  - fixed calculation of wasted CPU/Memory

## [0.1.1] - 2023-10-30 Running offline from eventlog feature
- Create SparkScopeApp with CLI to run SparkScope as standalone app 
  - reads application context, spark conf and events from eventLog  
  - can be run on finished or running spark application

## [0.1.0] - 2023-10-10 Initial Release
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
