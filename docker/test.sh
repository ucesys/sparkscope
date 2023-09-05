# SPARK SHELL
curl https://raw.githubusercontent.com/spark-examples/spark-scala-examples/master/src/main/resources/test.txt --output /tmp/alice.txt
curl https://repos.spark-packages.org/qubole/sparklens/0.3.2-s_2.11/sparklens-0.3.2-s_2.11.jar --output /tmp/sparklens-0.3.2-s_2.11.jar
spark-shell --jars /tmp/sparklens-0.3.2-s_2.11.jar --conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby

# SPARK SUBMIT
curl https://repo1.maven.org/maven2/org/apache/spark/spark-examples_2.10/1.1.1/spark-examples_2.10-1.1.1.jar --output /tmp/examples.jar
curl https://repos.spark-packages.org/qubole/sparklens/0.3.2-s_2.11/sparklens-0.3.2-s_2.11.jar --output /tmp/sparklens-0.3.2-s_2.11.jar
spark-submit --jars /tmp/sparklens-0.3.2-s_2.11.jar --conf spark.extraListeners=com.qubole.sparklens.QuboleJobListener --conf spark.driver.extraJavaOptions=-Dderby.system.home=/tmp/derby --class org.apache.spark.examples.SparkPi --master spark://spark-master:7077 --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=/tmp/spark-events /tmp/examples.jar 100
