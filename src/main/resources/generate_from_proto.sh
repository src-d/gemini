#!/bin/bash

# Generates code from proto files

set -e

# Python server

echo "Installing grpcio"
pip3 install grpcio==1.10.0 grpcio-tools==1.10.0
echo "Generating Python code from .proto to src/main/python/pb"
python3 -m grpc_tools.protoc -Isrc/main/proto \
--python_out=src/main/python/pb --grpc_python_out=src/main/python/pb \
src/main/proto/service.proto \
src/main/proto/github.com/gogo/protobuf/gogoproto/gogo.proto \
src/main/proto/gopkg.in/bblfsh/sdk.v1/uast/generated.proto

# Scala client

echo "Downloading scalapbc"
wget https://github.com/scalapb/ScalaPB/releases/download/v0.7.1/scalapbc-0.7.1.zip
unzip scalapbc-0.7.1.zip
echo "Generating Scala code from .proto to src/main/scala"
scalapbc-0.7.1/bin/scalapbc -Isrc/main/proto \
--scala_out=flat_package,grpc:src/main/scala \
src/main/proto/service.proto \
src/main/proto/github.com/gogo/protobuf/gogoproto/gogo.proto \
src/main/proto/gopkg.in/bblfsh/sdk.v1/uast/generated.proto
echo "Cleanup"
rm scalapbc-0.7.1.zip
rm -rf scalapbc-0.7.1
