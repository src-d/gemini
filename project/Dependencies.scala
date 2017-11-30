import sbt._

object Dependencies {
  lazy val scalaLib = "org.scala-lang" % "scala-library" % "2.11.11"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.1"
  lazy val scoverage = "org.scoverage" %% "scalac-scoverage-plugin" % "1.3.1"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.2.0"
  lazy val spark = "org.apache.spark" %% "spark-core" % "2.2.0"
  lazy val fixNetty = "io.netty" % "netty-all" % "4.1.11.Final"
  lazy val engine = "tech.sourced" % "engine" % "0.1.12" classifier "slim"
  lazy val jgit = "org.eclipse.jgit" % "org.eclipse.jgit" % "4.9.0.201710071750-r"
  lazy val cassandra =  "org.apache.cassandra" % "cassandra-all" % "3.2"
  lazy val cassandraDriver = "com.datastax.cassandra" % "cassandra-driver-core" % "3.1.4" classifier "shaded"
  lazy val cassandraSparkConnector = "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.5"
  lazy val cassandraSparkConnectorEmbedded = "com.datastax.spark" %% "spark-cassandra-connector-embedded" % "2.0.5"
}
