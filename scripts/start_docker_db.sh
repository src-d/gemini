#!/bin/bash

# CI convenience script to start a DB in Docker container.
# Skips starting DB for jobs that do not need it i.e stylecheck

E_BAD_DOCKER=2
image="scylladb/scylla:2.0.0"

set -ev

if [[ -z "${STYLE_CHECK}" ]]; then
  echo "Starting Docker image ${image}"
  docker run --name db -p 9042:9042 -d "${image}" \
    --broadcast-address 127.0.0.1 --listen-address 0.0.0.0 --broadcast-rpc-address 127.0.0.1 \
    --memory 2G --smp 1

  if [[ "$?" -ne 0 ]]; then
    echo "Unable to start Docker ${file_list}" >&2
    exit "${E_BAD_DOCKER}"
  fi
else
  echo "Skip starting Docker image ${image}"
fi

docker ps
