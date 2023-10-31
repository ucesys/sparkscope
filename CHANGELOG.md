# Changelog

## [0.1.3] - 2023-10-30
- Create SparkScopeApp with CLI to run SparkScope as standalone app 
  - reads application context, spark conf and events from eventLog  
  - can be run on finished or running spark application

## [0.1.2] - 2023-10-10
- Rename package from sparkscope to sparkscope-spark2
- Bump Scala version from 2.11.8 to 2.11.12
- Handle gently case for master=local where executorId=driver

## [0.1.1] - 2023-10-06
- configuration:
  - sparkscope will prioritize csv metrics SparkConf configuration and use metrics.properties files as last-resort
- html-report:
  - remove core-hours and heap gb-hours from warnings

## [0.1.0] - 2023-09-29
Initial SparkScope release:
- Charts:
  - heap & non-heap usage charts
  - cpu utilization charts
  - charts for driver, executors and aggregated charts for whole application
- Stats:
  - heap & non-heap usage stats
  - cpu utilization and memory utlization stats
  - stats for driver, executors and aggregated stats for whole application
  - CPU and Heap Memory Waste stats