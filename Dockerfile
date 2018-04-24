FROM debian:stretch-slim

ARG APACHE_SPARK_VERSION=2.2.0
ARG HADOOP_VERSION=2.7
ARG DUMP_INIT_VERSION=1.2.1
ENV SPARK_NAME=spark-${APACHE_SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV DUMP_INIT_DEB_NAME=dumb-init_${DUMP_INIT_VERSION}_amd64.deb

ENV SPARK_DIR /opt/${SPARK_NAME}
ENV SPARK_HOME /usr/local/spark

WORKDIR /gemini
COPY ./src/main/python/community-detector/requirements.txt /gemini/src/main/python/community-detector/requirements.txt

RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-8-jre-headless openjdk-8-jdk ca-certificates-java curl \
    python3 python3-pip python3-igraph && \
    pip3 install setuptools wheel && \
    pip3 install --only-binary=numpy,scipy -r src/main/python/community-detector/requirements.txt && \
    rm -rf /var/lib/apt/* && \
    curl http://d3kbcqa49mib13.cloudfront.net/${SPARK_NAME}.tgz | \
    tar xzf - -C /opt && \
    curl -L -o ${DUMP_INIT_DEB_NAME} \
    https://github.com/Yelp/dumb-init/releases/download/v${DUMP_INIT_VERSION}/${DUMP_INIT_DEB_NAME} && \
    dpkg -i dumb-init_*.deb && \
    rm ${DUMP_INIT_DEB_NAME} && \
    apt-get clean

RUN ln -s $SPARK_DIR $SPARK_HOME

COPY . /gemini

RUN ./sbt assemblyPackageDependency && ./sbt assembly && ./sbt package

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["bash", "-c", "sleep infinity & wait"]
