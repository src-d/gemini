#!/bin/bash

# A helper script to launch a Spark standalone master server and one worker

set -e

if [[ "$#" -ne 2 ]]; then
    echo "usage) $0 [Hostname to listen on] [Port to listen on]"
    echo "   eg) $0 127.0.0.1 7077"
    exit 1
fi

MASTER_HOST=$1
MASTER_PORT=$2

if [[ -z "${SPARK_HOME}" ]]; then
  echo "SPARK_HOME is not set"
  exit 1
fi

$SPARK_HOME/sbin/start-master.sh -h $MASTER_HOST -p $MASTER_PORT
$SPARK_HOME/sbin/start-slave.sh $MASTER_HOST:$MASTER_PORT