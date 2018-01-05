FROM debian:stretch-slim

ARG APACHE_SPARK_VERSION=2.2.0
ARG HADOOP_VERSION=2.7
ARG DUMP_INIT_VERSION=1.2.1
ENV SPARK_NAME=spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV DUMP_INIT_DEB_NAME=dumb-init_${DUMP_INIT_VERSION}_amd64.deb

ENV SPARK_DIR /opt/${SPARK_NAME}
ENV SPARK_HOME /usr/local/spark

RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-8-jre-headless  ca-certificates-java curl && \
    rm -rf /var/lib/apt/* && \
    curl http://d3kbcqa49mib13.cloudfront.net/${SPARK_NAME}.tgz | \
    tar xzf - -C /opt && \
    curl -L -o ${DUMP_INIT_DEB_NAME} \
    https://github.com/Yelp/dumb-init/releases/download/v${DUMP_INIT_VERSION}/${DUMP_INIT_DEB_NAME} && \
    dpkg -i dumb-init_*.deb && \
    rm ${DUMP_INIT_DEB_NAME} && \
    apt-get clean

RUN ln -s $SPARK_DIR $SPARK_HOME

WORKDIR /gemini
COPY . /gemini

RUN ./sbt assembly && ./sbt package

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["bash", "-c", "sleep infinity & wait"]
