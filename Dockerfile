FROM debian:stretch-slim

ARG APACHE_SPARK_VERSION=2.2.0
ARG HADOOP_VERSION=2.7
ENV SPARK_NAME=spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}

ENV SPARK_DIR /opt/${SPARK_NAME}
ENV SPARK_HOME /usr/local/spark

RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-8-jre-headless  ca-certificates-java curl && \
    rm -rf /var/lib/apt/* && \
    curl http://d3kbcqa49mib13.cloudfront.net/${SPARK_NAME}.tgz | \
    tar xzf - -C /opt && \
    mkdir -p /root/.sbt/launchers/0.13.13 && \
    curl -L -o /root/.sbt/launchers/0.13.13/sbt-launch.jar http://repo.typesafe.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.13.13/sbt-launch.jar && \
    apt-get remove -y curl && \
    apt-get clean

RUN ln -s $SPARK_DIR $SPARK_HOME

WORKDIR /gemini
COPY . /gemini

RUN ./sbt assembly && ./sbt package

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
