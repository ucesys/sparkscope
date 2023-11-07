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
csv hdfs metrics driver
```bash
spark-submit \
--jars /tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.driver.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
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
/tmp/jars/spark-examples_2.10-1.1.1.jar 500
```

csv hdfs metrics driver & executors   
*custom metrics sink/source need to be passed via --files and executor.extraclasspath, --jars is not enough
```bash
spark-submit \
--master spark://spark-master:7077 \
--jars /tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--files /tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
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
/tmp/jars/spark-examples_2.10-1.1.1.jar 500
```
csv hdfs metrics executors(--files only)
```bash
spark-submit \
--master spark://spark-master:7077 \
--files /tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--conf spark.driver.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
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
/tmp/jars/spark-examples_2.10-1.1.1.jar 500
```

hdfs metrics
```bash
spark-submit \
--master spark://spark-master:7077 \
--jars /tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--files /tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.1-hdfs-metrics-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.directory=file:///tmp/hdfs-metrics \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 500
```
### Running SparkScope as standalone app
```agsl
java -cp /tmp/jars/sparkscope-spark3-0.1.1-SNAPSHOT.jar:./jars/* com.ucesys.sparkscope.SparkScopeApp --event-log /tmp/spark-events/app-20231102142859-0005
```