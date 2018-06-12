#!/bin/bash

# Prepares tarball for release
# the list is quite huge, but we will improve it later
set -e

dst="gemini-$VERSION"

# prepare directories
mkdir -p "${dst}/target"
mkdir -p "${dst}/scripts"
mkdir -p "${dst}/src/main/resources"
mkdir -p "${dst}/src/main/python"

# need this to get version :/
cp build.sbt "${dst}/"

# copy java build results (jars) and resources
cp target/*.jar "${dst}/target/"
cp -r src/main/resources/* "${dst}/src/main/resources/"

# copy python
cp -r src/main/python/feature-extractor "${dst}/src/main/python/"
cp -r src/main/python/community-detector "${dst}/src/main/python/"

# copy scripts
cp hash "${dst}/"
cp query "${dst}/"
cp report "${dst}/"
cp feature_extractor "${dst}/"
cp scripts/common_params.sh "${dst}/scripts/"

# copy documentation
cp LICENSE "${dst}/"

# create archive
tar -cvzf gemini_$VERSION.tar.gz "${dst}/"
