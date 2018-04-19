#!/bin/bash

# A helper script to prepare the python runtime in Travis CI

sudo apt-get -qq update
sudo apt-get -y install python3-pip
sudo pip3 install --upgrade pip setuptools wheel
pip3 install --user --only-binary=numpy,scipy -r src/main/python/community-detector/requirements.txt pytest