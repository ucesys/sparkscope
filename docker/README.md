[![Gitter](https://badges.gitter.im/qubole-sparklens/community.svg)](https://gitter.im/qubole-sparklens/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

# Sparklens testing #


### Spark 2.4

Start spark master and worker containers. Worker container also contains spark history server.
```
docker compose -f docker/spark-2.4.6-docker-compose.yml up
```
Check Master and Spark History server UI:
- [Master UI:](http://localhost:8080/)   
- [Spark History UI](http://localhost:18080/)


Exec into worker container
```
docker exec -it spark-worker bash
```

Download sparklens, spark-examples, http-mime jars(http-mime needed to test email generation feature)
```
curl https://repo1.maven.org/maven2/org/apache/spark/spark-examples_2.10/1.1.1/spark-examples_2.10-1.1.1.jar --output /tmp/spark-examples.jar
curl https://repos.spark-packages.org/qubole/sparklens/0.3.2-s_2.11/sparklens-0.3.2-s_2.11.jar --output /tmp/sparklens-0.3.2-s_2.11.jar
curl https://repo1.maven.org/maven2/org/apache/httpcomponents/httpmime/4.5.14/httpmime-4.5.14.jar --output /tmp/httpmime-4.5.14.jar
```
Run sample application
```
spark-submit \
--jars /tmp/sparklens-0.3.2-s_2.11.jar \
--class org.apache.spark.examples.SparkPi \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener \
--conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
/tmp/spark-examples.jar 1000
```

Sparklens report should be generated to stdout

*Run with email generation(fails due to qubole sparklens endpoint being down)  

```
spark-submit \
--jars /tmp/sparklens-0.3.2-s_2.11.jar,/tmp/httpmime-4.5.14.jar \
--class org.apache.spark.examples.SparkPi \
--master spark://spark-master:7077 \
--conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener \
--conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby \
--conf spark.eventLog.enabled=true \
--conf spark.eventLog.dir=/tmp/spark-events \
--conf spark.sparklens.report.email=piotr.sobczak@ucesys.com
/tmp/spark-examples.jar 1000
```
