#!/bin/bash

# Prepares tarball for release
# the list is quite huge, but we will improve it later

# prepare directories
mkdir -p gemini-$VERSION/target
mkdir -p gemini-$VERSION/project
mkdir -p gemini-$VERSION/src/main/resources
mkdir -p gemini-$VERSION/src/main/python

# copy java build results (jars) and resources
cp target/*.jar gemini-$VERSION/target/
cp -r src/main/resources/* gemini-$VERSION/src/main/resources/

# copy sbt
cp project/build.properties gemini-$VERSION/project/
cp project/Dependencies.scala gemini-$VERSION/project/
cp project/plugins.sbt gemini-$VERSION/project/
cp sbt gemini-$VERSION/
cp build.sbt gemini-$VERSION/

# copy python
cp -r src/main/python/feature-extractor gemini-$VERSION/src/main/python/
cp -r src/main/python/community-detector gemini-$VERSION/src/main/python/

# copy scripts
cp hash gemini-$VERSION/
cp query gemini-$VERSION/
cp report gemini-$VERSION/
cp feature_extractor gemini-$VERSION/

# copy documentation
cp LICENCE gemini-$VERSION/

# create archive
tar -cvzf gemini_$VERSION.tar.gz gemini-$VERSION/
