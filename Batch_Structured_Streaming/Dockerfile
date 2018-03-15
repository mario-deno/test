FROM mesosphere/spark:2.1.0-2.2.1-1-hadoop-2.6
ENV SPARK_HOME /opt/spark/dist
ENV SPARK_SUBMIT_PORT 5050
ENV SPARK_CLASS com.sky.dap.App2
ENV SPARK_MASTER mesos://leader.mesos
ENV SPARK_JAVA_OPTS="-Dconfig.resource=test.conf -Dspark.mesos.driver.frameworkId=struct_str_avro_2.2.1"
ENV SUBMIT_ARGS="--conf spark.executor.home=/opt/spark/dist \
--conf spark.mesos.executor.docker.image=mesosphere/spark:2.1.0-2.2.1-1-hadoop-2.6 \
--conf spark.ssl.noCertVerification=true \
--conf spark.cores.max=5 \
--conf spark.driver.cores=2 \
--conf spark.driver.memory=2g \
--conf spark.executor.cores=1 \
--conf spark.executor.memory=2g \
--conf spark.mesos.driver.frameworkId=driver-struct_str \
--conf spark.app.name=struct-str_avro"
ADD ./target/Batch_Structured_Streaming-1.0-SNAPSHOT-jar-with-dependencies.jar
CMD $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER:$SPARK_SUBMIT_PORT --deploy-mode client $SUBMIT_ARGS --class $SPARK_CLASS Batch_Structured_Streaming-1.0-SNAPSHOT-jar-with-dependencies.jar 

