# SparkScope #

SparkScope is a monitoring and profiling tool for Spark Applications. It allows to review resource allocation, utiization, and demand timeline as it took place during Spark application execution. SparkScope presents information using visual charts which allow to
- find bottlenecks in application execution,
- reconcile resource demand and supply,
- fine tune Spark application for desired objectives.

It is implemented as SparkListener which means that it runs inside driver and listens for Spark events.
SparkScope utilizes csv metrics produced by custom SparkScopeCsvSink and supports multiple storage types.

## SparkScope Report
SparkScope produces reports in the following formats
- html
- json

SparkScope HTML reports contains the following features:
- Stats:
  - Application Info
  - Application Stats
  - Resource Stats
  - Executor Stats
  - Driver Stats
<img src="https://github.com/ucesys/sparkscope/blob/main/assets/stats.png" width="3800"></img>  

- Charts:
  - total % of utilized cpu/heap charts
<img src="https://github.com/ucesys/sparkscope/blob/main/assets/chart-util1.png" width="3800"></img>

  - total utilization vs allocation cpu/heap charts
<img src="https://github.com/ucesys/sparkscope/blob/main/assets/chart-util2.png" width="3800"></img>

  - number of tasks vs CPU capacity and number of executors
<img src="https://github.com/ucesys/sparkscope/blob/main/assets/chart-tasks.png" width="3800"></img>

  - heap and non-heap charts for executors
<img src="https://github.com/ucesys/sparkscope/blob/main/assets/chart-executors.png" width="3800"></img>

  - heap and non-heap charts for driver
<img src="https://github.com/ucesys/sparkscope/blob/main/assets/chart-driver.png" width="3800"></img>  
- Warnings:
  - Low CPU utilization warning
  - Low Memory utilization warning
  - Data Spills from memory to disk warning
  - Long time spent in Garbage Collection warning
<img src="https://github.com/ucesys/sparkscope/blob/main/assets/warnings.png" width="3800"></img>  

## Compatibility matrix

|                           | Spark 2 (spark2 branch) | Spark 3 (main branch) |
|---------------------------|-------------------------|-----------------------|
| Scala version             | 2.11.12                 | 2.12.18               |
| compatible JDK versions   | 7, 8                    | 8, 11, 17             |
| compatible Spark versions | 2.3, 2.4                | 3.2, 3.3, 3.4, 3.5    |

## Compatible storage types:
- S3
- HDFS
- MaprFS
- NFS/local

## Tested environments:
- Hadoop Yarn(Client and Cluster deploy modes)
- Spark Standalone cluster



## Spark application configuration

| parameter                                    | type      | sample values                                   | description                                                                                                                  |
|----------------------------------------------|-----------|-------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| spark.extraListeners                         | mandatory | com.ucesys.sparkscope.SparkScopeJobListener     | Spark listener class                                                                                                         
| spark.metrics.conf.driver.source.jvm.class   | mandatory | org.apache.spark.metrics.source.JvmSource       | jvm metrics source for driver                                                                                                
| spark.metrics.conf.executor.source.jvm.class | mandatory | org.apache.spark.metrics.source.JvmSource       | jvm metrics source for executor                                                                                              
| spark.metrics.conf.*.sink.csv.class          | mandatory | org.apache.spark.metrics.sink.SparkScopeCsvSink | csv sink class                                                                                                               
| spark.metrics.conf.*.sink.csv.period         | mandatory | 5                                               | period of metrics spill                                                                                                      
| spark.metrics.conf.*.sink.csv.unit           | mandatory | seconds                                         | unit of period of metrics spill                                                                                              |
| spark.metrics.conf.*.sink.csv.directory      | mandatory | s3://my-bucket/path/to/metrics                  | path to metrics directory, can be s3,hdfs,maprfs,local                                                                       |
| spark.metrics.conf.*.sink.csv.region         | optional  | us-east-1                                       | aws region, required for s3 storage                                                                                          |
| spark.metrics.conf.*.sink.csv.appName        | optional  | MyApp                                           | application name, also used for grouping metrics                                                                             |
| spark.sparkscope.report.html.path            | optional  | s3://my-bucket/path/to/html/report/dir          | path to which SparkScope html report will be saved                                                                           |
| spark.sparkscope.report.json.path            | optional  | s3://my-bucket/path/to/json/report/dir          | path to which SparkScope json report will be saved                                                                           |
| spark.sparkscope.log.path                    | optional  | s3://my-bucket/path/to/log/dir                  | path to which SparkScope logs will be saved                                                                                  |
| spark.sparkscope.log.level                   | optional  | DEBUG, INFO, WARN, ERROR                        | logging level for SparkScope logs                                                                                            |
| spark.sparkscope.diagnostics.enabled         | optional  | true/false                                      | set to false to disable submitting diagnostics, default=true.                                                          |
| spark.sparkscope.metrics.dir.driver          | optional  | s3://my-bucket/path/to/metrics                  | path to driver csv metrics relative to driver, defaults to "spark.metrics.conf.driver.sink.csv.directory" property value     |
| spark.sparkscope.metrics.dir.executor        | optional  | s3://my-bucket/path/to/metrics                  | path to executor csv metrics relative to driver, defaults to "spark.metrics.conf.executor.sink.csv.directory" property value |



## Attaching SparkScope to Spark applications
Notes:
- One can choose to put all spark.metrics.conf properties is a metrics.properties file
- Using custom sink(SparkScopeCsvSink) requires adding jar to driver & executors and extending their classpaths.
- --files(spark.files) option should be used
- --jars(spark.jars) option will only make the Sink available for the driver

#### Storing metrics to S3
```bash
spark-submit \
--master yarn \
--files ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--driver-class-path ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=s3://<bucket-name>/<path-to-metrics-dir> \
--conf spark.metrics.conf.*.sink.csv.region=<region> \
--conf spark.metrics.conf.*.sink.csv.appName=My-App \
--conf spark.sparkscope.report.html.path=s3://<bucket-name>/<path-to-html-report-dir> \
--class org.apache.spark.examples.SparkPi \
./spark-examples_2.10-1.1.1.jar 5000
```

#### Storing metrics to Hadoop(hdfs/maprfs)
```bash
spark-submit \
--master yarn \
--files ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--driver-class-path ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=hdfs://<path-to-metrics-dir> \
--conf spark.metrics.conf.*.sink.csv.appName=My-App \
--conf spark.sparkscope.report.html.path=hdfs://<path-to-html-report-dir> \
--class org.apache.spark.examples.SparkPi \
./spark-examples_2.10-1.1.1.jar 5000
```

#### Storing metrics to NFS/locally
```bash
spark-submit \
--master yarn \
--files ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--driver-class-path ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=<path-to-metrics-dir> \
--conf spark.metrics.conf.*.sink.csv.appName=My-App \
--conf spark.sparkscope.report.html.path=<path-to-html-report-dir> \
--class org.apache.spark.examples.SparkPi \
./spark-examples_2.10-1.1.1.jar 5000
```
#### Using metrics.properties file instead of spark.metrics.conf.* properties:
Instead of specifying spark.metrics.conf.* as separate properties, we can also specify them in metrics.properties file:
```bash
# Enable CsvSink for all instances by class name
*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink

# Polling period for the CsvSink
*.sink.csv.period=5

# Unit of the polling period for the CsvSink
*.sink.csv.unit=seconds

# Polling directory for CsvSink
*.sink.csv.directory=hdfs:///tmp/csv-metrics

# JVM SOURCE
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource
```

And specifying path to metrics.properties file in spark-submit command:
```bash
spark-submit \
--master yarn \
--files ./sparkscope-spark3-0.1.9-SNAPSHOT.jar,./metrics.properties \
--driver-class-path ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.metrics.conf=./metrics.properties \
--conf spark.sparkscope.report.html.path=hdfs://<path-to-html-report-dir> \
--class org.apache.spark.examples.SparkPi \
./spark-examples_2.10-1.1.1.jar 5000
```

### Running SparkScope as standalone app for running/finished Spark Application
Your application needs to have eventLog and metrics configured(but not the listener)
```agsl
spark-submit \
--master yarn \
--files ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--driver-class-path ./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=./sparkscope-spark3-0.1.9-SNAPSHOT.jar \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=<path-to-metrics-dir> \
--conf spark.metrics.conf.*.sink.csv.appName=My-App \
--class org.apache.spark.examples.SparkPi \
./spark-examples_2.10-1.1.1.jar 5000
```
Running sparkscope as java-app
```agsl
java \
-cp ./sparkscope-spark3-0.1.9-SNAPSHOT.jar:$(hadoop classpath) \
com.ucesys.sparkscope.SparkScopeApp \
--event-log <path-to-event-log> \
--html-path <path-to-html-report-dir> \
--json-path <path-to-json-report-dir> \
--log-path <path-to-log-dir> \
--log-level <logging level> \
--diagnostics <true/false> \
--region <aws-region>
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
