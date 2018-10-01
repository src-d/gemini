#!/bin/bash

# Installs python dependencies for ./report on Travis CI

set -x

pip3 install -q --only-binary=numpy,scipy -r src/main/python/community-detector/requirements.txt pytest
