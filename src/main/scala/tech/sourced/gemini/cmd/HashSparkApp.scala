package tech.sourced.gemini.cmd

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import tech.sourced.engine.provider.RepositoryRDDProvider
import tech.sourced.gemini.Gemini

import scala.util.Properties

case class HashAppConfig(reposPath: String = "",
                         limit: Int = 0,
                         host: String = Gemini.defaultCassandraHost,
                         port: Int = Gemini.defaultCassandraPort,
                         keyspace: String = Gemini.defautKeyspace,
                         bblfshHost: String = Gemini.defaultBblfshHost,
                         bblfshPort: Int = Gemini.defaultBblfshPort,
                         feHost: String = Gemini.defaultFeHost,
                         fePort: Int = Gemini.defaultFePort,
                         format: String = "siva",
                         sparkMemory: String = "4gb",
                         sparkParallelism: Int = 8,
                         verbose: Boolean = false)

/**
  * Apache Spark app that applied LSH to given repos, using source{d} Engine.
  */
object HashSparkApp extends App with Logging {
  val repoFormats = Seq("siva", "bare", "standard")
  val printLimit = 100

  val parser = new Parser[HashAppConfig]("./hash") {
    head("Gemini Hasher")
    note("Hashes given set of Git repositories, either from FS or as .siva files.")

    opt[String]('h', "host")
      .action((x, c) => c.copy(host = x))
      .text("host is Cassandra host")
    opt[Int]('p', "port")
      .action((x, c) => c.copy(port = x))
      .text("port is Cassandra port")
    opt[String]('k', "keyspace")
      .action((x, c) => c.copy(keyspace = x))
      .text("keyspace is Cassandra keyspace")
    opt[String]("bblfsh-host")
      .action((x, c) => c.copy(bblfshHost = x))
      .text("host is babelfish server host")
    opt[Int]("bblfsh-port")
      .action((x, c) => c.copy(bblfshPort = x))
      .text("port is babelfish server port")
    opt[String]("features-extractor-host")
      .action((x, c) => c.copy(feHost = x))
      .text("host is features-extractor server host")
    opt[Int]("features-extractor-port")
      .action((x, c) => c.copy(fePort = x))
      .text("port is features-extractor server port")
    opt[Int]('l', "limit")
      .action((x, c) => c.copy(limit = x))
      .text("limit on the number of processed repositories")
    opt[String]('f', "format")
      .valueName(repoFormats.mkString(" | "))
      .withFallback(() => "siva")
      .validate(x =>
        if (repoFormats contains x) success else failure(s"format must be one of " + repoFormats.mkString(" | "))
      )
      .action((x, c) => c.copy(format = x))
      .text("format of the stored repositories")
    opt[String]("spark-memory")
      .action((x, c) => c.copy(sparkMemory = x))
      .text("amount of memory to use for the spark driver process")
    opt[Int]("spark-parallelism")
      .action((x, c) => c.copy(sparkParallelism = x))
      .text("default number of spark partitions in RDDs returned by transformations like " +
        "join, reduceByKey, and parallelize when not set by user.")
    opt[Unit]('v', "verbose")
      .action((_, c) => c.copy(verbose = true))
      .text("producing more verbose debug output")
    arg[String]("<path-to-git-repos>")
      .required()
      .action((x, c) => c.copy(reposPath = x))
      .text("path to git repositories. Clones in local FS or Siva files in HDFS are supported.")
  }

  parser.parseWithEnv(args, HashAppConfig()) match {
    case Some(config) =>
      if (config.verbose) {
        LogManager.getRootLogger.setLevel(Level.WARN)
      }
      val sparkMaster = Properties.envOrElse("MASTER", "local[*]")
      println(s"Running Hashing as Apache Spark job, master: $sparkMaster")

      val spark = SparkSession.builder()
        .master(sparkMaster)
        .config("spark.driver.memory", config.sparkMemory)
        .config("spark.default.parallelism", config.sparkParallelism)
        .config("spark.cassandra.connection.host", config.host)
        .config("spark.cassandra.connection.port", config.port)
        .config("spark.tech.sourced.bblfsh.grpc.host", config.bblfshHost)
        .config("spark.tech.sourced.bblfsh.grpc.port", config.bblfshPort)
        .config("spark.tech.sourced.featurext.grpc.host", config.feHost)
        .config("spark.tech.sourced.featurext.grpc.port", config.fePort)
        .getOrCreate()

      val reposPath = config.reposPath
      val repos = listRepositories(reposPath, config.format, spark, config.limit)
      printRepositories(reposPath, repos)

      val gemini = Gemini(spark, log, config.keyspace)
      log.info("Checking DB schema")
      CassandraConnector(spark.sparkContext).withSessionDo { cassandra =>
        gemini.applySchema(cassandra)
      }

      gemini.hash(reposPath, config.limit, config.format)
      println("Done")

    case None =>
      System.exit(2)
  }

  private def printRepositories(reposPath: String, repos: Array[String]): Unit = {
    val numToPrint = Math.min(repos.length, printLimit)
    println(s"Hashing ${repos.length} repositories in: '$reposPath' " +
      s"${if (numToPrint < repos.length) s"(only $numToPrint shown)"}")
    repos
      .take(numToPrint)
      .foreach(repo => println(s"\t$repo"))
  }

  def listRepositories(path: String, format: String, ss: SparkSession, limit: Int = 0): Array[String] = {
    val paths = RepositoryRDDProvider(ss.sparkContext).get(path, format).map(_.root).collect()
    if (limit <= 0) paths else paths.take(limit)
  }

}
