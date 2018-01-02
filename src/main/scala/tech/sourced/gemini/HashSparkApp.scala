package tech.sourced.gemini

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging
import org.apache.log4j.{Level, LogManager}

import scala.util.Properties

case class HashAppConfig(reposPath: String = "",
                         limit: Int = 0,
                         host: String = Gemini.defaultCassandraHost,
                         port: Int = Gemini.defaultCassandraPort,
                         format: String = "siva",
                         verbose: Boolean = false)

/**
  * Apache Spark app that applied LSH to given repos, using source{d} Engine.
  */
object HashSparkApp extends App with Logging {
  val repoFormats = Seq("siva", "bare", "standard")

  val parser = new scopt.OptionParser[HashAppConfig]("./hash") {
    head("Gemini Hasher")
    note("Hashes given set of Git repositories, either from FS or as .siva files.")

    opt[String]('h', "host")
      .action((x, c) => c.copy(host = x))
      .text("host is Cassandra host")
    opt[Int]('p', "port")
      .action((x, c) => c.copy(port = x))
      .text("port is Cassandra port")
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
    opt[Unit]('v', "verbose")
      .action((_, c) => c.copy(verbose = true))
      .text("producing more verbose debug output")
    arg[String]("<path-to-git-repos>")
      .required()
      .action((x, c) => c.copy(reposPath = x))
      .text("path to git repositories. Clones in local FS or Siva files in HDFS are supported.")
  }

  parser.parse(args, HashAppConfig()) match {
    case Some(config) =>
      val reposPath = config.reposPath

      val spark = SparkSession.builder()
        .master(Properties.envOrElse("MASTER", "local[*]"))
        .config("spark.cassandra.connection.host", config.host)
        .config("spark.cassandra.connection.port", config.port)
        .getOrCreate()

      if (config.verbose) {
        LogManager.getRootLogger().setLevel(Level.INFO)
      }

      val gemini = Gemini(spark, log, "hashes")
      CassandraConnector(spark.sparkContext).withSessionDo { cassandra =>
        gemini.applySchema(cassandra)
      }
      val filesToWrite = gemini.hash(reposPath, config.limit, config.format)
      gemini.save(filesToWrite)
      println("Done")

    case None =>
      System.exit(2)
  }
}
