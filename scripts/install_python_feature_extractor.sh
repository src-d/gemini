#!/bin/bash

# Installs python dependencies for ./feature_extractor on Travis CI

set -x

sudo apt-get -qq update
# libsnappy is needed for sourced-ml
sudo apt-get --no-install-recommends -y install libsnappy1 libsnappy-dev

if [[ -d "${SPARK_HOME}/python" ]] ; then
    # to avoid downloading Apache Spark though pip, see https://github.com/src-d/ml#use-existing-apache-spark
    echo "Using PySpark from ${SPARK_HOME} instead of downloading it"
    pip3 install -e "${SPARK_HOME}/python"
fi

pip3 install --upgrade pip setuptools wheel
pip3 install -q -r "src/main/python/feature-extractor/requirements.txt"
