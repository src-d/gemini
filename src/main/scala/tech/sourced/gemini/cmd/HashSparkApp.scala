package tech.sourced.gemini.cmd

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import tech.sourced.engine.provider.RepositoryRDDProvider
import tech.sourced.gemini.Gemini

import scala.util.Properties

case class HashAppConfig(
  reposPath: String = "",
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
  docFreqFile: String = "",
  verbose: Boolean = false,
  mode: String = Gemini.fileSimilarityMode,
  replace: Boolean = false,
  gcsKeyFile: String = "",
  awsKey: String = "",
  awsSecret: String = "",
  awsS3Endpoint: String = ""
)

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
    opt[String]('m', "mode")
      .valueName(Gemini.similarityModes.mkString(" | "))
      .withFallback(() => Gemini.fileSimilarityMode)
      .validate(x =>
        if (Gemini.similarityModes contains x) {
          success
        } else {
          failure(s"similarity mode must be one of: " + Gemini.similarityModes.mkString(" | "))
        })
      .action((x, c) => c.copy(mode = x))
      .text("similarity mode to be used")
    opt[String]("doc-freq-file")
      .action((x, c) => c.copy(docFreqFile = x))
      .text("path to file with feature frequencies")
    opt[Unit]("replace")
      .action((x, c) => c.copy(replace = true))
      .text("replace results of previous hashing")
    opt[String]("gcs-keyfile")
      .action((x, c) => c.copy(gcsKeyFile = x))
      .text("path to JSON keyfile for authentication in Google Cloud Storage")
    opt[String]("aws-key")
      .action((x, c) => c.copy(awsKey = x))
      .text("AWS access keys")
    opt[String]("aws-secret")
      .action((x, c) => c.copy(awsSecret = x))
      .text("AWS access secret")
    opt[String]("aws-s3-endpoint")
      .action((x, c) => c.copy(awsS3Endpoint = x))
      .text("region S3 endpoint")
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

      if (config.gcsKeyFile.nonEmpty) {
        spark.sparkContext.hadoopConfiguration.set("google.cloud.auth.service.account.json.keyfile", config.gcsKeyFile)
      }

      // AWS S3 combo
      // The problem is we use old version of spark&hadoop which has 4 issues:
      // 1. It brings as dependency old amazon-aws package
      // which requires separate flag to enable support for current aws protocol
      spark.sparkContext.hadoopConfiguration.set("com.amazonaws.services.s3.enableV4", "true")
      // 2. Only NativeS3FileSystem works correctly with new protocol
      spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      // 3. The client is configured to use the default S3A service endpoint
      // but for v4 protocol it must be set to the region endpoint bucket belongs to
      if (config.awsS3Endpoint.nonEmpty) {
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", config.awsS3Endpoint)
      }
      // 4. Glob (from jgit-spark-connector) with key&secret in URL isn't supported by current version
      // $ ./hash s3a://key:token@bucket/repos/
      // Error: "Wrong FS: s3a://key:token@bucket/repos/*, expected: s3a://key:token@bucket"
      if (config.awsKey.nonEmpty) {
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsAccessKeyId", config.awsKey)
      }
      if (config.awsSecret.nonEmpty) {
        spark.sparkContext.hadoopConfiguration.set("fs.s3a.awsSecretAccessKey", config.awsSecret)
      }

      val reposPath = config.reposPath
      val repos = listRepositories(reposPath, config.format, spark, config.limit)
      printRepositories(reposPath, repos)

      val gemini = Gemini(spark, log, config.keyspace)
      log.info("Checking DB schema")
      CassandraConnector(spark.sparkContext).withSessionDo { cassandra =>
        gemini.applySchema(cassandra)

        if (config.replace) {
          gemini.cleanDB(cassandra, config.mode)
        } else if (!gemini.isDBEmpty(cassandra, config.mode)) {
          println("Database keyspace is not empty! Hashing may produce wrong results. " +
            "Please choose another keyspace or pass the --replace option")
          System.exit(2)
        }
      }

      gemini.hash(reposPath, config.limit, config.format, config.mode, config.docFreqFile)
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
