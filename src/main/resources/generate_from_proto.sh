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

SCALAPBC_VERSION=0.7.1
SCALAPBC_NAME=scalapbc-$SCALAPBC_VERSION
if [[ ! -f "${SCALAPBC_NAME}/bin/scalapbc" ]]; then
    echo "Downloading scalapbc"
    wget "https://github.com/scalapb/ScalaPB/releases/download/v${SCALAPBC_VERSION}/${SCALAPBC_NAME}.zip"
    unzip ${SCALAPBC_NAME}.zip
fi;

echo "Generating Scala code from .proto to src/main/scala"
${SCALAPBC_NAME}/bin/scalapbc -Isrc/main/proto \
--scala_out=grpc:src/main/scala \
src/main/proto/service.proto \
src/main/proto/github.com/gogo/protobuf/gogoproto/gogo.proto \
src/main/proto/gopkg.in/bblfsh/sdk.v1/uast/generated.proto
