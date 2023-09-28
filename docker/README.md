[![Gitter](https://badges.gitter.im/qubole-sparklens/community.svg)](https://gitter.im/qubole-sparklens/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

# Sparkscope testing
### Build sparkscope  
With sbt
```bash
sbt package
```
or maven
```bash
mvn package
```
### Put sparkscope and spark-examples jars into docker/lib 
```bash
mkdir -p docker/lib
cp ./target/sparkscope-<VERSION>.jar docker/lib
curl https://repo1.maven.org/maven2/org/apache/spark/spark-examples_2.10/1.1.1/spark-examples_2.10-1.1.1.jar --output ./docker/lib/spark-examples_2.10-1.1.1.jar
```

### Start spark master and worker
```bash
docker-compose -f docker/spark-2.4.6-docker-compose.yml up
```
### Start Spark history server inside worker container
```bash
docker exec spark-worker start-history-server.sh
```
Check Master and Spark History server UI:
- [Master UI:](http://localhost:8080/)
- [Spark History UI](http://localhost:18080/)

### Submitting spark applications
Exec into worker container
```bash
docker exec -it spark-worker bash
```
Create a directory for csv metrics
```bash
mkdir -p /tmp/csv-metrics
```
Run spark-submit
```bash
spark-submit \
--jars /tmp/jars/sparkscope-<VERSION>.jar \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf=/tmp/metrics.properties \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=900m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 5000
```
Sparkscope report summary should be printed out to the console:
```
28/09/2023 11:20:07 INFO [SparkScope]            ____              __    ____
28/09/2023 11:20:07 INFO [SparkScope]           / __/__  ___ _____/ /__ / __/_ ___  ___  ___
28/09/2023 11:20:07 INFO [SparkScope]          _\ \/ _ \/ _ `/ __/  '_/_\ \/_ / _ \/ _ \/__/ 
28/09/2023 11:20:07 INFO [SparkScope]         /___/ .__/\_,_/_/ /_/\_\/___/\__\_,_/ .__/\___/
28/09/2023 11:20:07 INFO [SparkScope]            /_/                             /_/ 
28/09/2023 11:20:07 INFO [SparkScope] 

Executor stats:
Executor heap size: 800MB
Max heap memory utilization by executor: 356MB(44.55%)
Average heap memory utilization by executor: 197MB(24.71%)
Max non-heap memory utilization by executor: 49MB
Average non-heap memory utilization by executor: 43MB
28/09/2023 11:20:07 INFO [SparkScope] 

Driver stats:
Driver heap size: 910
Max heap memory utilization by driver: 313MB(34.45%)
Average heap memory utilization by driver: 244MB(26.83%)
Max non-heap memory utilization by driver: 68MB
Average non-heap memory utilization by driver: 66MB
28/09/2023 11:20:07 INFO [SparkScope] 

Cluster Memory stats: 
Average Cluster heap memory utilization: 24.71% / 395MB
Max Cluster heap memory utilization: 40.07% / 641MB
heapGbHoursAllocated: 0.0195
heapGbHoursAllocated=(executorHeapSizeInGb(0.78125)*combinedExecutorUptimeInSec(90s))/3600
heapGbHoursWasted: 0.0048
heapGbHoursWasted=heapGbHoursAllocated(0.0195)*heapUtilization(0.2471)

28/09/2023 11:20:07 INFO [SparkScope] 
Cluster CPU stats: 
Total CPU utilization: 82.20%
coreHoursAllocated: 0.0500
coreHoursAllocated=(executorCores(2)*combinedExecutorUptimeInSec(90s))/3600
coreHoursWasted: 0.0411
coreHoursWasted=coreHoursAllocated(0.0500)*cpuUtilization(0.8220)
28/09/2023 11:20:07 INFO [SparkScope] SparkScope analysis took 0.082s
Wrote HTML report file to /tmp//app-20230928111918-0002.html

```
# Sparklens testing #

Download sparklens, spark-examples, http-mime jars(http-mime needed to test email generation feature) 
```
mkdir -p docker/lib
curl https://repo1.maven.org/maven2/org/apache/spark/spark-examples_2.10/1.1.1/spark-examples_2.10-1.1.1.jar --output ./docker/lib/spark-examples_2.10-1.1.1.jar
curl https://repos.spark-packages.org/qubole/sparklens/0.3.2-s_2.11/sparklens-0.3.2-s_2.11.jar --output ./docker/lib/sparklens-0.3.2-s_2.11.jar
curl https://repo1.maven.org/maven2/org/apache/httpcomponents/httpmime/4.5.14/httpmime-4.5.14.jar --output ./docker/lib/httpmime-4.5.14.jar
```

Run sample application, Sparklens report should be generated to stdout:
```
spark-submit \
--jars /tmp/jars/sparklens-0.3.2-s_2.11.jar  \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.ucesys.sparklens.QuboleJobListener \
--conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf=/tmp/metrics.properties \
--conf spark.executor.cores=1 \
--conf spark.executor.memory=900m \
--conf spark.executor.instances=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 5000
```

Run with email generation(fails due to qubole sparklens endpoint being down)  

```
spark-submit \
--jars /tmp/jars/sparklens-0.3.2-s_2.11.jar,/tmp/jars/httpmime-4.5.14.jar \
--class org.apache.spark.examples.SparkPi \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener \
--conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.sparklens.report.email=piotr.sobczak@ucesys.com
/tmp/jars/spark-examples_2.10-1.1.1.jar 1000
```
