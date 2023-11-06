# Sparkscope testing

### Spark 3.2.0 JDK 8 test
```bash
docker-compose -f docker/spark-3.2.4-docker-compose.yml up
```

### Spark 3.2.4 JDK 11 test
```bash
docker-compose -f docker/spark-3.2.4-docker-compose.yml up
```

### Spark 3.3.2 JDK 17 test
```bash
docker-compose -f docker/spark-3.3.2-docker-compose.yml up
```

### Spark 3.4.1 JDK 17 test
```bash
docker-compose -f docker/spark-3.4.1-docker-compose.yml up
```

### Spark 3.5.0 JDK 17 test
```bash
docker-compose -f docker/spark-3.5.0-docker-compose.yml up
```

### Attaching SparkScope as Spark Listener via Spark submit
```bash
docker exec -it spark-worker bash
```
Low Memory Util
```bash
spark-submit \
--jars /tmp/jars/sparkscope-spark3-0.1.1-SNAPSHOT.jar \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf=/tmp/metrics.properties \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 1000
```
Optimized Memory Util
```bash
spark-submit \
--jars /tmp/jars/sparkscope-spark3-0.1.1-SNAPSHOT.jar \
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
/tmp/jars/spark-examples_2.10-1.1.1.jar 1000
```
No metrics.properties
```bash
spark-submit \
--jars /tmp/jars/sparkscope-spark3-0.1.1-SNAPSHOT.jar \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.CsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=/tmp/csv-metrics \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 1000
```
csv hdfs metrics
```bash
spark-submit \
--jars /tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.driver.sink.csv.class=org.apache.spark.metrics.sink.HdfsCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=/tmp/csv-metrics \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 1000
```
hdfs metrics
```bash
spark-submit \
--jars /tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.driver.sink.hdfs.class=org.apache.spark.metrics.sink.HDFSSink \
--conf spark.metrics.conf.driver.sink.hdfs.dir=hdfs:///tmp/ \
--conf spark.metrics.conf.driver.sink.hdfs.unit=seconds \
--conf spark.metrics.conf.driver.sink.hdfs.pollPeriod=5 \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 1000
```
### Running SparkScope as standalone app
```agsl
java -cp /tmp/jars/sparkscope-spark3-0.1.1-SNAPSHOT.jar:./jars/* com.ucesys.sparkscope.SparkScopeApp --event-log /tmp/spark-events/app-20231102142859-0005
```