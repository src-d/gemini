#!/bin/bash

# Prepares tarball for release
# the list is quite huge, but we will improve it later

mkdir -p gemini-$VERSION/target
mkdir -p gemini-$VERSION/project
mkdir -p gemini-$VERSION/src/main/resources
cp target/*.jar gemini-$VERSION/target/
cp sbt gemini-$VERSION/
cp build.sbt gemini-$VERSION/
cp hash gemini-$VERSION/
cp query gemini-$VERSION/
cp report gemini-$VERSION/
cp project/build.properties gemini-$VERSION/project/
cp project/Dependencies.scala gemini-$VERSION/project/
cp project/plugins.sbt gemini-$VERSION/project/
cp -r src/main/resources/* gemini-$VERSION/src/main/resources/
tar -cvzf gemini_$VERSION.tar.gz gemini-$VERSION/