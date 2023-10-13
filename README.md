# SparkScope #

SparkScope is a monitoring and profiling tool for Spark Applications. 
It is implemented as SparkListener which means that it runs inside driver and listens for spark events.
SparkScope uses csv metrics produced by [CsvSink](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/metrics/sink/CsvSink.scala).
SparkScope was forked from Qubole profiling tool [Sparklens](https://github.com/qubole/sparklens).

SparkScope html report contains the following features:
- Charts:
  - heap & non-heap usage charts
  - cpu utilization charts
  - charts for driver, executors and aggregated charts for whole application
- Stats:
  - heap & non-heap usage stats
  - cpu utilization and memory utlization stats
  - stats for driver, executors and aggregated stats for whole application
  - CPU and Heap Memory Waste stats

## Compatibiltiy matrix

|                           | spark 2 (sparkscope/main) | spark 3 (sparkscope/spark3) |
|---------------------------|---------------------------|-----------------------------|
| scala version             | 2.11.12                   | 2.12.18                     |
| compatible JDK versions   | 7, 8                      | 8, 11, 17                   |
| compatible Spark versions | 2.3, 2.4                  | 3.2, 3.3, 3.4, 3.5          |

## Tested environments:
- Hadoop Yarn(Client and Cluster deploy modes)
- Spark Standalone cluster 



## Spark application configuration

| parameter                               |                                                                                                                                                                   |
|-----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.extraListeners                    | com.ucesys.sparkscope.SparkScopeJobListener                                                                                                                       |
| spark.jars                              | path to SparkScope jar                                                                                                                                            |
| spark.metrics.conf                      | path to metrics.properties with CSV sinks configuration                                                                                                           |
| spark.sparkscope.html.path              | path to which SparkScope html report will be saved                                                                                                                |
| spark.sparkscope.metrics.dir.driver     | path to driver csv metrics relative to driver, if unspecified property driver.sink.csv.directory or *.sink.csv.directory from spark.metrics.conf will be used     |
| spark.sparkscope.metrics.dir.executor   | path to executor csv metrics relative to driver, if unspecified property executor.sink.csv.directory or *.sink.csv.directory from spark.metrics.conf will be used |

## Attaching SparkScope to Spark applications(without metrics.properties file)
Sample spark-submit command:
```bash
spark-submit \
--master yarn \
--jars ./sparkscope_2.11-0.1.1.jar  \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.CsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=/tmp/csv-metrics \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.sparkscope.metrics.dir.driver=/tmp/csv-metrics \
--conf spark.sparkscope.metrics.dir.executor=/tmp/csv-metrics \
--conf spark.sparkscope.html.path=./ \
--class org.apache.spark.examples.SparkPi \
./spark-examples_2.10-1.1.1.jar 5000
```

## Attaching SparkScope to Spark applications(with metrics.properties file)
metrics.properties configuration example
```bash
# Enable CsvSink for all instances by class name
*.sink.csv.class=org.apache.spark.metrics.sink.CsvSink

# Polling period for the CsvSink
*.sink.csv.period=5

# Unit of the polling period for the CsvSink
*.sink.csv.unit=seconds

# Polling directory for CsvSink
*.sink.csv.directory=/tmp/csv-metrics

# Polling period for the CsvSink specific for the worker instance
worker.sink.csv.period=5

# Unit of the polling period for the CsvSink specific for the worker instance
worker.sink.csv.unit=seconds

# JVM SOURCE
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource
```

Sample spark-submit command:
```bash
spark-submit \
--master yarn \
--jars ./sparkscope_2.11-0.1.1.jar  \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.metrics.conf=./metrics.properties \
--files ./metrics.properties \
--conf spark.sparkscope.metrics.dir.driver=/tmp/csv-metrics \
--conf spark.sparkscope.metrics.dir.executor=/tmp/csv-metrics \
--conf spark.sparkscope.html.path=./ \
--class org.apache.spark.examples.SparkPi \
./spark-examples_2.10-1.1.1.jar 5000
```

## SparkScope summary:

SparkScope analysis summary should be printed out to the console:
```
28/09/2023 01:20:22 INFO [SparkScope] SparkScope analysis took 0.052s
28/09/2023 01:20:22 INFO [SparkScope] 
     ____              __    ____
    / __/__  ___ _____/ /__ / __/_ ___  ___  ___
   _\ \/ _ \/ _ `/ __/  '_/_\ \/_ / _ \/ _ \/__/
  /___/ .__/\_,_/_/ /_/\_\/___/\__\_,_/ .__/\___/
     /_/                             /_/      v0.1.1

28/09/2023 01:20:22 INFO [SparkScope] Executor stats:
Executor heap size: 800MB
Max heap memory utilization by executor: 286MB(35.80%)
Average heap memory utilization by executor: 156MB(19.56%)
Max non-heap memory utilization by executor: 49MB
Average non-heap memory utilization by executor: 35MB

28/09/2023 01:20:22 INFO [SparkScope] Driver stats:
Driver heap size: 910
Max heap memory utilization by driver: 262MB(28.87%)
Average heap memory utilization by driver: 207MB(22.78%)
Max non-heap memory utilization by driver: 67MB
Average non-heap memory utilization by driver: 65MB

28/09/2023 01:20:22 INFO [SparkScope] Cluster Memory stats: 
Average Cluster heap memory utilization: 19.56% / 156MB
Max Cluster heap memory utilization: 35.80% / 286MB
heapGbHoursAllocated: 0.0033
heapGbHoursAllocated=(executorHeapSizeInGb(0.78125)*combinedExecutorUptimeInSec(15s))/3600
heapGbHoursWasted: 0.0006
heapGbHoursWasted=heapGbHoursAllocated(0.0033)*heapUtilization(0.1956)

28/09/2023 01:20:22 INFO [SparkScope] Cluster CPU stats: 
Total CPU utilization: 68.35%
coreHoursAllocated: 0.0042
coreHoursAllocated=(executorCores(1)*combinedExecutorUptimeInSec(15s))/3600
coreHoursWasted: 0.0029
coreHoursWasted=coreHoursAllocated(0.0042)*cpuUtilization(0.6835)

28/09/2023 01:20:22 INFO [SparkScope] Wrote HTML report file to /tmp/app-20230928132004-0012.html
```

## SparkScope report
Last line of the report shows where html report was saved:
`28/09/2023 01:20:22 INFO [SparkScope] Wrote HTML report file to /tmp/app-20230928132004-0012.html`

SparkScope html report contains:
- Aggregated application CPU/Memory statistics & charts
- Driver and Executor CPU/Memory statistics & charts
- Warnings concerning application resource usage
- Sparklens report
- SparkScope logs
- Spark config
