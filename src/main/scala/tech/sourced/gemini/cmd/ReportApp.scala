package tech.sourced.gemini.cmd

import com.datastax.driver.core.Cluster
import tech.sourced.gemini._
import tech.sourced.gemini.util.Logger


case class ReportAppConfig(
  host: String = Gemini.defaultCassandraHost,
  port: Int = Gemini.defaultCassandraPort,
  keyspace: String = Gemini.defautKeyspace,
  format: String = ReportApp.defaultFmt,
  ccDirPath: String = ".",
  verbose: Boolean = false,
  mode: String = Gemini.fileSimilarityMode
)

object ReportApp extends App {
  val defaultFmt = ""
  val defaultFmtGroupBy = "use-group-by"
  val condensedFmt = "condensed"

  val parser = new Parser[ReportAppConfig]("./report") {
    head("Gemini Report")
    note("Finds duplicated files among hashed repositories." +
      "It uses as many queries as distinct files are stored in the database")

    opt[String]('h', "host")
      .action((x, c) => c.copy(host = x))
      .text("host is Cassandra host")
    opt[Int]('p', "port")
      .action((x, c) => c.copy(port = x))
      .text("port is Cassandra port")
    opt[String]('k', "keyspace")
      .action((x, c) => c.copy(keyspace = x))
      .text("keyspace is Cassandra keyspace")
    opt[String]('o', "cc-output")
      .action((x, c) => c.copy(ccDirPath = x))
      .text("directory path to output parquet files with connected components")
    opt[Unit]('v', "verbose")
      .action((_, c) => c.copy(verbose = true))
      .text("producing more verbose debug output")
    opt[String]("format")
      .valueName("use-group-by or condensed")
      .action((x, c) => c.copy(format = x))
      .text("Only for Apache Cassandra database\n" +
        "use-group-by - use as many queries as unique duplicate files are found, plus one.\n" +
        "condensed - use only one query to find the duplicates.")
  }

  parser.parseWithEnv(args, ReportAppConfig()) match {
    case Some(config) =>
      val log = Logger("gemini", config.verbose)
      println(s"Reporting all similar files")

      log.info("Creating Cassandra connection")
      //TODO(bzz): wrap to CassandraConnector(config).withSessionDo { session =>
      val cluster = Cluster.builder()
        .addContactPoint(config.host)
        .withPort(config.port)
        .build()
      val cassandra = cluster.connect()
      val gemini = Gemini(null, log, config.keyspace)
      log.info("Checking DB schema")
      gemini.applySchema(cassandra)

      val ReportResult(duplicates, similarities) = gemini.report(cassandra, config.format, config.ccDirPath)

      print(duplicates)
      printCommunities(similarities)

      log.info("Closing DB connection")
      cassandra.close()
      cluster.close()

    case None =>
      System.exit(2)
  }

  def print(report: ReportDuplicates): Unit = {
    report match {
      case e if e.empty() => println(s"No duplicated files found.")
      case ReportGrouped(v) => println(s"Duplicated files found:\n\t" + (v mkString "\n\t"))
      case ReportExpandedGroup(v) =>
        v.foreach { item =>
          val count = item.size
          println(s"$count duplicates:\n\t" + (item mkString "\n\t") + "\n")
        }
    }
  }

  def printCommunities(report: Iterable[Iterable[SimilarItem]]): Unit = {
    if (report.isEmpty) {
      println(s"No similarities found.")
    } else {
      report.foreach { community =>
        val count = community.size
        val typeName = community.head match {
          case SimilarFunc(_, _, _) => "functions"
          case _ => "files"
        }
        println(s"$count similar ${typeName}:\n\t${community.mkString("\n\t")}\n")
      }
    }
  }
}
