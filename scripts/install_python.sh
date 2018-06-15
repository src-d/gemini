#!/bin/bash

# A helper script to prepare the python runtime in Travis CI

sudo apt-get -qq update
# libsnappy is needed for sourced-ml
sudo apt-get --no-install-recommends -y install python3-pip libsnappy1 libsnappy-dev
# pip==9.0.1 same version as sourced-ml uses
sudo pip3 install --upgrade pip==9.0.1 setuptools wheel
pip3 install -q -I --user --only-binary=numpy,scipy -r src/main/python/community-detector/requirements.txt pytest
