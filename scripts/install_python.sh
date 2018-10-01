#!/bin/bash

# A helper script to prepare the python runtime in Travis CI

set -x

sudo apt-get -qq update
# libsnappy is needed for sourced-ml
sudo apt-get --no-install-recommends -y install libsnappy1 libsnappy-dev
pip3 install -q -I --user --only-binary=numpy,scipy -r src/main/python/community-detector/requirements.txt pytest
