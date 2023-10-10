# Sparkscope testing

### Spark 3.2.0 JDK 8 test
Run Spark 3.2.0 / JDK 11 container
```bash
docker-compose -f docker/spark-3.2.4-docker-compose.yml up
```

```bash
docker exec -it spark-worker bash
```

```bash
spark-submit \
--jars /tmp/jars/sparkscope-spark3-0.1.0-SNAPSHOT.jar \
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
/tmp/jars/spark-examples_2.10-1.1.1.jar 2000
```

### Spark 3.2.4 JDK 11 test
Run Spark 3.2.4 / JDK 11 container
```bash
docker-compose -f docker/spark-3.2.4-docker-compose.yml up
```

```bash
docker exec -it spark-worker bash
```

```bash
spark-submit \
--jars /tmp/jars/sparkscope-spark3-0.1.0.jar \
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
