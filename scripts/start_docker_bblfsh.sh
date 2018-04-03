#!/bin/bash

# CI convenience script to start a bblfsh server in Docker container.
# Skips starting server for jobs that do not need it i.e stylecheck

E_BAD_DOCKER=2
image="bblfsh/bblfshd:v2.4.2"

set -ev

if [[ -z "${STYLE_CHECK}" ]]; then
  echo "Starting Docker image ${image}"
  docker run --name bblfshd --privileged -p 9432:9432 -d "${image}"

  if [[ "$?" -ne 0 ]]; then
    echo "Unable to start Docker ${file_list}" >&2
    exit "${E_BAD_DOCKER}"
  fi

  docker exec -it bblfshd bblfshctl driver install --recommended
else
  echo "Skip starting Docker image ${image}"
fi

docker ps
