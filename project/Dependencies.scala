import sbt._

object Dependencies {
  lazy val scalaLib = "org.scala-lang" % "scala-library" % "2.11.11"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
  lazy val scoverage = "org.scoverage" %% "scalac-scoverage-plugin" % "1.3.1"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0"
  lazy val spark = "org.apache.spark" %% "spark-core" % "2.2.0"
  lazy val fixNetty = "io.netty" % "netty-all" % "4.1.17.Final"
  lazy val fixNewerHadoopClient = "org.apache.hadoop" % "hadoop-client" % "2.7.2"
  lazy val engine = "tech.sourced" % "engine" % "0.7.0" classifier "slim"
  lazy val jgit = "org.eclipse.jgit" % "org.eclipse.jgit" % "4.9.0.201710071750-r"
  lazy val cassandraSparkConnector = "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.6"
  lazy val cassandraDriverMetrics = "com.codahale.metrics" % "metrics-core" % "3.0.2"
  lazy val scopt = "com.github.scopt" %% "scopt" % "3.7.0"
  lazy val slf4jApi = "org.slf4j" % "slf4j-api" % "1.7.5"
  lazy val log4j12 = "log4j" % "log4j" % "1.2.17"
  lazy val log4jBinding = "org.slf4j" % "slf4j-log4j12" % "1.7.25"
  lazy val avro = "org.apache.avro" % "avro" % "1.7.7"
  lazy val parquetAvro = "org.apache.parquet" % "parquet-avro" % "1.9.0"
  // compatible version with spark 2.2
  // hadoop-aws will bring hadoop-common package with it which is needed for gemini itself
  lazy val hadoopAws = ("org.apache.hadoop" % "hadoop-aws" % "2.7.2")
    // excludes conflict dependencies with cassandra driver
    // similar to:
    // https://github.com/datastax/spark-cassandra-connector/blob/v2.0.6/project/SparkCassandraConnectorBuild.scala#L184
    .exclude("com.sun.jersey", "jersey-server")
    .exclude("commons-beanutils", "commons-beanutils-core")
  lazy val scalapb = "com.thesamet.scalapb" %% "scalapb-runtime" % "0.8.4"
  lazy val scalapbGrpc = "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % "0.8.4"
  lazy val ioGrpc = "io.grpc" % "grpc-netty" % "1.10.0"
  lazy val commonsMath = "org.apache.commons" % "commons-math3" % "3.6.1"
  lazy val bblfshClient = "org.bblfsh" % "bblfsh-client" % "1.8.2"
  lazy val scalaJsonParser = "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.6.7"
  lazy val gcs = "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop2-1.9.11"
}
