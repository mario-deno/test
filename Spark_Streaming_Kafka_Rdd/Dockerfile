FROM mesosphere/spark:1.0.7-2.1.0-hadoop-2.6
ENV SPARK_HOME /opt/spark/dist
ENV SPARK_CLASS com.sky.dap.eu_portability.IdentityListener
ENV SPARK_MASTER mesos://leader.mesos:5050
ENV SUBMIT_ARGS="--conf spark.executor.home=/opt/spark/dist \
--conf spark.mesos.executor.docker.image=artifacts.ath.bskyb.com:5003/skydap/eu_portability_identity_listener \
--conf spark.ssl.noCertVerification=true \
--conf spark.cores.max=2 \
--conf spark.driver.cores=2 \
--conf spark.driver.memory=2g \
--conf spark.executor.cores=1 \
--conf spark.executor.memory=2g"
ADD ./target/identity_listener-1.0-SNAPSHOT-jar-with-dependencies.jar /identity_listener-1.0-SNAPSHOT-jar-with-dependencies.jar
CMD $SPARK_HOME/bin/spark-submit --master $SPARK_MASTER --deploy-mode client --class $SPARK_CLASS $SUBMIT_ARGS /identity_listener-1.0-SNAPSHOT-jar-with-dependencies.jar

