# Changelog

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
