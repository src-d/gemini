# Gemini [![Build Status](https://travis-ci.org/src-d/gemini.svg?branch=master)](https://travis-ci.org/src-d/gemini) [![codecov](https://codecov.io/gh/src-d/gemini/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/gemini)
> Find similar code in Git repositories

Gemini is a tool for searching for duplicate 'items' in the many source code repositories.
Supported granularity level or items are:
 - repositories (WIP)
 - files
 - functions (TBD)

## Pre-requests
 - JVM 1.8
 - Apache Cassandra or ScyllaDB
 - Apache Spark
 - Go (optional)


```bash
# save some repos in .siva files using Borges
echo -e "https://github.com/src-d/borges.git\nhttps://github.com/erizocosmico/borges.git" > repo-list.txt

# get Borges from https://github.com/src-d/borges/releases
borges pack --loglevel=debug --workers=2 --to=./repos -f repo-list.txt

# start Apache Cassandra
docker run -p 9042:9042 \
  --name cassandra -d rinscy/cassandra:3.11.1

# or ScyllaDB \w workaround https://github.com/gocql/gocql/issues/987
docker run -p 9042:9042 --volume $(pwd)/scylla:/var/lib/scylla \
  --name some-scylla -d scylladb/scylla:2.0.0 \
  --broadcast-address 127.0.0.1 --listen-address 0.0.0.0 --broadcast-rpc-address 127.0.0.1 \
  --memory 2G --smp 1

# to get access to DB for development
docker exec -it some-scylla cqlsh

# apply schema from ./src/main/resourced/schema.cql
```


## Run
```
./hash   <path-to-repos-or-siva-files>
./query  <path-to-file>
./report <path-to-repos-or-siva-files>
```

By default, all commands are going to use
 - **Apache Cassandra or ScyllaDB** instance available at `localhost:9042`
 - **Apache Spark**, available though `$SPARK_HOME`


### Hash
To pre-process number of repositories for a quick finding of the duplicates run

```
./hash $(pwd)/src/test/resources/siva
```

Input format of the repositories is the same as in [src-d/Engine](https://github.com/src-d/engine).


### Query
To find all duplicate of the single file run

```
./query <path-to-single-file>
```

There is an example of a client in a Golang under [src/main/go](src/main/go)

### Report
To find all duplicate files in all repositories run

```
./report <path-to-repos-or-siva-files>
```

All repositories must be [hashed](#hash) before.



### External Apache Spark cluster
Just set url to the Spark Master though env var
```
MASTER="spark://<spark-master-url>" ./hash <path>
```

### CLI arguments

```
 --db <url to Cassandra>, default localhost:9042
```

# Dev

Build, fatJar for Apache Spark (hash, report)
```
./sbt assembly
```

Build, for query
```
./sbt package
```

Tests (with embedded Cassandra)
```
./sbt test
```
