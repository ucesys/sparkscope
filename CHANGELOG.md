# Changelog
## [0.1.1] - 2023-10-06
Initial SparkScope release:
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