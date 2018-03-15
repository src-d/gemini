#!/bin/bash

# Generates code from proto files

set -e

python3 -m grpc_tools.protoc -I../proto \
--python_out=../python/pb --grpc_python_out=../python/pb \
../proto/service.proto \
../proto/github.com/gogo/protobuf/gogoproto/gogo.proto \
../proto/gopkg.in/bblfsh/sdk.v1/uast/generated.proto
