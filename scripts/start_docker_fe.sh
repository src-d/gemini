#!/bin/bash

# CI convenience script to start a feature extractor in Docker container.
# Skips starting container for jobs that do not need it i.e stylecheck

E_BAD_DOCKER=2
image="srcd/gemini-fe"

set -ev

if [[ -z "${STYLE_CHECK}" ]]; then
  echo "Starting Docker image ${image}"
  docker run --name fe -e PYTHONHASHSEED=0 -p 9001:9001 -d "${image}"

  if [[ "$?" -ne 0 ]]; then
    echo "Unable to start Docker ${file_list}" >&2
    exit "${E_BAD_DOCKER}"
  fi
else
  echo "Skip starting Docker image ${image}"
fi

docker ps
