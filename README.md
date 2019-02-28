# Gemini [![Build Status](https://travis-ci.org/src-d/gemini.svg?branch=master)](https://travis-ci.org/src-d/gemini) [![codecov](https://codecov.io/gh/src-d/gemini/branch/master/graph/badge.svg)](https://codecov.io/gh/src-d/gemini)
> Find similar code in Git repositories

Gemini is a tool for searching for similar 'items' in source code repositories.
The supported granularity levels for items are:

- repositories (TBD)
- files
- functions

Gemini is based on its sister research project codenamed [Apollo](https://github.com/src-d/apollo).

## Run

```
./hash   <path-to-repos-or-siva-files>
./query  <path-to-file>
./report
```

You would need to prefix commands with `docker-compose exec gemini` if you run it in docker. Read below how to start gemini in docker or standalone mode.

### Hash

To pre-process number of repositories for a quick finding of the duplicates run

```
./hash ./src/test/resources/siva
```

Input format of the repositories is the same as in [src-d/Engine](https://github.com/src-d/engine).

To pre-process repositories for search of similar functions run:

```
./hash -m func ./src/test/resources/siva
```

Besides local file system gemini support different [distributed storages](#distributed-storages).

### Query

To find all duplicate of the single file run

```
./query <path-to-single-file>
```

To find all similar function defined in a file run:

```
./query -m func <path-to-single-file>
```

If you are interested in similarities of only 1 function defined in the file you can run:

```
./query -m func <path-to-single-file>:<function name>:<line number where the function is defined>
```

### Report

To find all duplicate files and similar functions in all repositories run

```
./report
```

All repositories must be [hashed](#hash) before and a
[community detection library](src/main/python/community-detector/README.md#install-dependencies) installed.

## Requirements

### Docker

Start containers:

```bash
docker-compose up -d
```

Local directories `repositories` and `query` are available as `/repositories` and `/query` inside the container.

Examples:

```bash
docker-compose exec gemini ./hash /repositories
docker-compose exec gemini ./query /query/consumer.go
docker-compose exec gemini ./report
```


### Standalone

You would need:

 - JVM 1.8
 - Apache Cassandra or ScyllaDB
 - Apache Spark 2.2.x
 - Python 3
 - [Bblfshd](https://github.com/bblfsh/bblfshd/) v2.5.0+

By default, all commands are going to use
 - **Apache Cassandra or ScyllaDB** instance available at `localhost:9042`
 - **Apache Spark**, available though `$SPARK_HOME`

```bash
# save some repos in .siva files using Borges
echo -e "https://github.com/src-d/borges.git\nhttps://github.com/erizocosmico/borges.git" > repo-list.txt

# get Borges from https://github.com/src-d/borges/releases
borges pack --loglevel=debug --workers=2 --to=./repos -f repo-list.txt

# start Apache Cassandra
docker run -p 9042:9042 \
  --name cassandra -d rinscy/cassandra:3.11

# or ScyllaDB \w workaround https://github.com/gocql/gocql/issues/987
docker run -p 9042:9042 --volume $(pwd)/scylla:/var/lib/scylla \
  --name some-scylla -d scylladb/scylla:2.0.0 \
  --broadcast-address 127.0.0.1 --listen-address 0.0.0.0 --broadcast-rpc-address 127.0.0.1 \
  --memory 2G --smp 1

# to get access to DB for development
docker exec -it some-scylla cqlsh
```


### Configuration for Apache Spark

Use env variables to set memory for hash job:
```
export DRIVER_MEMORY=30g
export EXECUTOR_MEMORY=60g
```

To use a external cluster just set the URL to the Spark Master though an env var:
```
MASTER="spark://<spark-master-url>" ./hash <path>
```


### CLI arguments

All three commands accept parameters for database connection and logging:

 * `-h/--host` - cassandra/scylla db hostname, default `127.0.0.1`
 * `-p/--port` - cassandra/scylla db port, default `9042`
 * `-k/--keyspace` - cassandra/scylla db keyspace, default `hashes`
 * `-v/--verbose` - producing more verbose output, default `false`

For `query` and `hash` commands parameters for bblfsh/features extractor configuration are available:

 * `-m/--mode` - similarity modes: `file` or `function`, default `file`
 * `--bblfsh-host` - babelfish server host, default `127.0.0.1`
 * `--bblfsh-port` - babelfish server port, default `9432`
 * `--features-extractor-host` - features-extractor host, default `127.0.0.1`
 * `--features-extractor-port` - features-extractor port, default `9001`

Hash command specific arguments:

 * `-l/--limit` - limit the number of repositories to be processed. All repositories will be processed by default
 * `-f/--format` - format of the stored repositories. Supported input data formats that repositories could be stored in are `siva`, `bare` or `standard`, default `siva`
 * `--gcs-keyfile` - path to [JSON keyfile](https://cloud.google.com/storage/docs/authentication) for authentication in Google Cloud Storage

Report specific arguments:

 * `--output-format` - output format: text or json
 * `--cassandra` - Enable advanced cql queries for Apache Cassandra database


### Limitations

Currently gemini targets medium size repositories and datasets.

We set resonable defaults and pre-filtering rules to provide the best results for this case.
List of rules:

- Exclude binary files
- Exclude empty files from full duplication results
- Exclude files less than 500B from file-similarity results
- Similarity deduplication works only for [languages supported by babelfish](https://docs.sourced.tech/babelfish/languages) and syntactically correct files


### Performance tips

We recommend to run Spark with 10GB+ memory for each executer and for the driver. Gemini wouldn't benifit from more than 1 CPU per task.

Horizontal scaling doesn't work well for the first stage of the pipeline and depends on size of the biggest repositories in a dataset but the rest of pipeline scales well.


### Distributed storages

Gemini supports different distributed storages in local and cluster mode. It already includes all necessary jars as a part of fat jar.

#### HDFS

Path format to git repositories: `hdfs://hdfs-namenode/path`

To configure HDFS in local or cluster mode please consult [Hadoop documentation](https://hadoop.apache.org/docs/r2.7.2/index.html).

#### Google Cloud Storage

Path format to git repositories: `gs://bucket/path`

To connect to GCS locally use `--gcs-keyfile` flag with path to [JSON keyfile](https://cloud.google.com/storage/docs/authentication#service_accounts).

To use GCS in cluster mode please consult [Google Cloud Storage Connector documentation](https://github.com/GoogleCloudPlatform/bigdata-interop/blob/master/gcs/INSTALL.md#configure-hadoop).

#### Amazon Web Services S3

Path format to git repositories: `s3a://bucket/path`

To connect to S3 locally use following flags:
- `--aws-key` - AWS access keys
- `--aws-secret` - AWS access secret
- `--aws-s3-endpoint` - [region endpoint](https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region) of your S3 bucket

Due to some limitations passing key&secret as part of URI is not supported.

To use AWS S3 in cluster mode please consult [hadoop-aws documentation](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html#S3A)


### Known bugs

* Search for similarities in C# code isn't supported right now ([patch](https://github.com/smacker/gemini/commit/dcaebc295ff490d2800ef80af07a29925201a673) with workaround)
* Timeout for UAST extraction is relatevely low on real dataset according to our experience and it isn't configurable ([patch1](https://github.com/smacker/gemini/commit/cc5703169df640ff34bf35e2c8259216319f1cfb) and [path2](https://github.com/smacker/gemini/commit/342dd5074db5bd6bdeef2f6c855f8b5622b4b3ff) with workaround)
* For standard & bare format gemini prints wrong repositories listing ([issue](https://github.com/src-d/gemini/issues/199))


## Development

### Compile & Run
If env var `DEV` is set, `./sbt` is used to compile and run all non-Spark commands: `./hash` and `./report`.
This is a convenient for local development, as not requiring a separate "compile" step allows for a dev workflow
that is similar to experience with interpreted languages.

### Build
To build final .jars for all commands
```
./sbt assemblyPackageDependency
./sbt assembly
```
Instead of 1 fatJar we bulid 2, separating all the dependencies from actual application code to allow for
lower build times in case of simple changes.

### Test

To run tests, that rely
```
./sbt test
```

### Re-generate code
Latest generated code for gRPC is already checked in under `src/main/scala/tech/sourced/featurext`.
In case you update any of the `src/main/proto/*.proto`, you would need to generate gRPC code for Feature Extractors:
```
./src/main/resources/generate_from_proto.sh
```

To generate new protobuf messages fixtures for tests, you may use [bblfsh-sdk-tools](https://github.com/bblfsh/sdk):
```
bblfsh-sdk-tools fixtures -p .proto -l <LANG> <path-to-source-code-file>
```

## License

Copyright (C) 2018 source{d}.
This project is licensed under the [GNU General Public License v3.0](LICENSE).
