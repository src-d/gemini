#!/bin/bash

# Parse parameters for dependencies or use environment variable
#
# List of params is common for all commands:
# Database: DB_HOST/DB_PORT
# Bblfshd: BBLFSH_HOST/BBLFSH_PORT
# Feature extractor: FE_HOST/FE_PORT

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

# read arguments and override variable
case $key in
    -h|--host)
    DB_HOST="$2"
    shift # past argument
    shift # past value
    ;;
    -p|--port)
    DB_PORT="$2"
    shift # past argument
    shift # past value
    ;;
    --bblfsh-host)
    BBLFSH_HOST="$2"
    shift # past argument
    shift # past value
    ;;
    --bblfsh-port)
    BBLFSH_PORT="$2"
    shift # past argument
    shift # past value
    ;;
    --features-extractor-host)
    FE_HOST="$2"
    shift # past argument
    shift # past value
    ;;
    --features-extractor-port)
    FE_PORT="$2"
    shift # past argument
    shift # past value
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

# construct params to pass to sbt
COMMAND_PARAMS=()
if [ -n "$DB_HOST" ]; then COMMAND_PARAMS+=("--host" "$DB_HOST"); fi
if [ -n "$DB_PORT" ]; then COMMAND_PARAMS+=("--port" "$DB_PORT"); fi
# report doesn't need bblfsh or fe
if [[ ! $0 = *"report"* ]]; then
    if [ -n "$BBLFSH_HOST" ]; then COMMAND_PARAMS+=("--bblfsh-host" "$BBLFSH_HOST"); fi
    if [ -n "$BBLFSH_PORT" ]; then COMMAND_PARAMS+=("--bblfsh-port" "$BBLFSH_PORT"); fi
    if [ -n "$FE_HOST" ]; then COMMAND_PARAMS+=("--features-extractor-host" "$FE_HOST"); fi
    if [ -n "$FE_PORT" ]; then COMMAND_PARAMS+=("--features-extractor-port" "$FE_PORT"); fi
fi

COMMAND_PARAMS+=("${POSITIONAL[@]}")
