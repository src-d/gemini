#!/bin/bash

# Generates code from proto files

set -e

# Python server

pip3 install grpcio==1.10.0 grpcio-tools==1.10.0
python3 -m grpc_tools.protoc -Isrc/main/proto \
--python_out=src/main/python/pb --grpc_python_out=src/main/python/pb \
src/main/proto/service.proto \
src/main/proto/github.com/gogo/protobuf/gogoproto/gogo.proto \
src/main/proto/gopkg.in/bblfsh/sdk.v1/uast/generated.proto

# Scala client

wget https://github.com/scalapb/ScalaPB/releases/download/v0.7.1/scalapbc-0.7.1.zip
unzip scalapbc-0.7.1.zip
scalapbc-0.7.1/bin/scalapbc -Isrc/main/proto \
--scala_out=flat_package,grpc:src/main/scala \
src/main/proto/service.proto \
src/main/proto/github.com/gogo/protobuf/gogoproto/gogo.proto \
src/main/proto/gopkg.in/bblfsh/sdk.v1/uast/generated.proto
rm scalapbc-0.7.1.zip
rm -rf scalapbc-0.7.1
