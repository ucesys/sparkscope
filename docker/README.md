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

local metrics(--jars and --files)
*custom metrics sink/source need to be passed via --files and executor.extraclasspath, --jars is not enough
```bash
spark-submit \
--master spark://spark-master:7077 \
--jars /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--files /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=1 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=/tmp/csv-metrics \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.*.sink.csv.appName=WordCount \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=1 \
--conf spark.cores.max=4 \
--class com.ucesys.sparkscope.WordCount \
/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT-tests.jar /tmp/jars/long500mb.txt 10000
```
csv hdfs metrics executors(--files only)
```bash
spark-submit \
--master spark://spark-master:7077 \
--files /tmp/jars/sparkscope-spark3-0.1.2-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.2-SNAPSHOT.jar \
--conf spark.driver.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.2-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=hdfs:///tmp/csv-metrics \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 1000
```
s3 metrics
```bash
spark-submit \
--master spark://spark-master:7077 \
--jars /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--files /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=s3://ucesys-sparkscope-metrics/metrics/ \
--conf spark.metrics.conf.*.sink.csv.region=us-east-1 \
--conf spark.sparkscope.html.path=s3://ucesys-sparkscope-metrics/report/ \
--conf spark.metrics.conf.*.sink.csv.appName=WordCount \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.executor.cores=1 \
--conf spark.executor.memory=1000m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class com.ucesys.sparkscope.WordCount \
/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT-tests.jar /tmp/jars/test.txt 100
```

s3 metrics & eventlog s3
```bash
spark-submit \
--master spark://spark-master:7077 \
--jars /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--files /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=s3a://ucesys-sparkscope-metrics/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=5 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=s3://ucesys-sparkscope-metrics/metrics/ \
--conf spark.metrics.conf.*.sink.csv.region=us-east-1 \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=1 \
--conf spark.cores.max=4 \
--class org.apache.spark.examples.SparkPi \
/tmp/jars/spark-examples_2.10-1.1.1.jar 1000
```

s3 metrics & html report s3
```bash
spark-submit \
--master spark://spark-master:7077 \
--jars /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--files /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=1 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=s3://ucesys-sparkscope-metrics/metrics/ \
--conf spark.metrics.conf.*.sink.csv.region=us-east-1 \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.sparkscope.html.path=s3://ucesys-sparkscope-metrics/report \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class com.ucesys.sparkscope.WordCount \
/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT-tests.jar /tmp/jars/long1gb.txt 10000
```

s3 metrics & eventlog & html report s3
```bash
spark-submit \
--master spark://spark-master:7077 \
--jars /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--files /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.executor.extraClassPath=/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar \
--conf spark.extraListeners=com.ucesys.sparkscope.SparkScopeJobListener \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=s3a://ucesys-sparkscope-metrics/spark-events \
--conf spark.metrics.conf.*.sink.csv.class=org.apache.spark.metrics.sink.SparkScopeCsvSink \
--conf spark.metrics.conf.*.sink.csv.period=1 \
--conf spark.metrics.conf.*.sink.csv.unit=seconds \
--conf spark.metrics.conf.*.sink.csv.directory=s3://ucesys-sparkscope-metrics/metrics/ \
--conf spark.metrics.conf.*.sink.csv.region=us-east-1 \
--conf spark.metrics.conf.driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.metrics.conf.executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource \
--conf spark.sparkscope.html.path=s3://ucesys-sparkscope-metrics/report \
--conf spark.executor.cores=2 \
--conf spark.executor.memory=1800m \
--conf spark.executor.instances=2 \
--conf spark.cores.max=4 \
--class com.ucesys.sparkscope.WordCount \
/tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT-tests.jar ./long.txt 2000
```
### Running SparkScope as standalone app
```agsl
java -cp /tmp/jars/sparkscope-spark3-0.1.8-SNAPSHOT.jar:./jars/* com.ucesys.sparkscope.SparkScopeApp --event-log /tmp/spark-events/app-20231129134104-0001
```