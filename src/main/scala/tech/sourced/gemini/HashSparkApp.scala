package tech.sourced.gemini

import java.net.URI

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
  val printLimit = 100

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
        LogManager.getRootLogger.setLevel(Level.INFO)
      }

      val repos = listRepositories(reposPath, config.format, spark.sparkContext.hadoopConfiguration, config.limit)
      printRepositories(reposPath, repos)

      val gemini = Gemini(spark, log, "hashes")
      CassandraConnector(spark.sparkContext).withSessionDo { cassandra =>
        gemini.applySchema(cassandra)
      }

      gemini.hash(reposPath, config.limit, config.format)
      println("Done")

    case None =>
      System.exit(2)
  }

  private def printRepositories(reposPath: String, repos: Array[Path]): Unit = {
    println(s"Hashing ${repos.length} repositories in: $reposPath")
    if (repos.length < printLimit && repos.length > 0) {
      println("\t" + (repos mkString "\n\t"))
    }
  }

  private def listRepositories(path: String, format: String, conf: Configuration, limit: Int): Array[Path] = {
    val fs = FileSystem.get(new URI(path), conf)
    val p = new Path(path)

    val paths = format match {
      case "siva" => findSivaRecursive(p, fs)
      case _ => fs.listStatus(p).filter(_.isDirectory).map(_.getPath)
    }

    if (limit <= 0) paths else paths.take(limit)
  }

  // we don't filter files by extension here, because engine doesn't do it too
  // it will try to process any file
  private def findSivaRecursive(p: Path, fs: FileSystem): Array[Path] =
    fs.listStatus(p).flatMap{ file =>
      if (file.isDirectory) findSivaRecursive(file.getPath, fs) else Array(file.getPath)
    }
}
