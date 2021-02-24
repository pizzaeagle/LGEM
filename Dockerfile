FROM spark:spark-docker



COPY target/scala-2.12/lgem_2.12-0.1.jar /opt/spark/example/jars/lgem.jar
COPY *properties /opt/

ENV SPARK_HOME /opt/spark

WORKDIR /opt/spark/work-dir

ENTRYPOINT [ "/opt/entrypoint.sh" ]
