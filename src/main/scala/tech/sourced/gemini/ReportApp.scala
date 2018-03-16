package tech.sourced.gemini

import com.datastax.driver.core.Cluster

case class ReportAppConfig(host: String = Gemini.defaultCassandraHost,
                           port: Int = Gemini.defaultCassandraPort,
                           mode: String = ReportApp.defaultMode,
                           verbose: Boolean = false)

object ReportApp extends App {
  val defaultMode = ""
  val groupByMode = "use-group-by"
  val condensedMode = "condensed"

  val parser = new scopt.OptionParser[ReportAppConfig]("./report") {
    head("Gemini Report")
    note("Finds duplicated files among hashed repositories." +
      "It uses as many queries as distinct files are stored in the database")

    opt[String]('h', "host")
      .action((x, c) => c.copy(host = x))
      .text("host is Cassandra host")
    opt[Int]('p', "port")
      .action((x, c) => c.copy(port = x))
      .text("port is Cassandra port")
    opt[Unit]('v', "verbose")
      .action((_, c) => c.copy(verbose = true))
      .text("producing more verbose debug output")
    opt[String]("mode")
      .valueName("use-group-by or condensed")
      .action((x, c) => c.copy(mode = x))
      .text("Only for Apache Cassandra database\n" +
        "use-group-by - use as many queries as unique duplicate files are found, plus one.\n" +
        "condensed - use only one query to find the duplicates.")
  }

  parser.parse(args, ReportAppConfig()) match {
    case Some(config) =>
      val log = Logger("gemini", config.verbose)

      //TODO(bzz): wrap to CassandraConnector(config).withSessionDo { session =>
      val cluster = Cluster.builder()
        .addContactPoint(config.host)
        .withPort(config.port)
        .build()
      val cassandra = cluster.connect()
      val gemini = Gemini(null, log)
      gemini.applySchema(cassandra)

      val report = config.mode match {
        case `defaultMode` => gemini.report(cassandra)
        case `condensedMode` => gemini.reportCassandraCondensed(cassandra)
        case `groupByMode` => gemini.reportCassandraGroupBy(cassandra)
      }

      cassandra.close()
      cluster.close()

      print(report)
    case None =>
      System.exit(2)
  }

  def print(report: Report): Unit = {
    report match {
      case e if e.empty() => println(s"No duplicates found.")
      case ReportGrouped(v) => println(s"Duplicates found:\n\t" + (v mkString "\n\t"))
      case ReportExpandedGroup(v) =>
        v.foreach { item =>
          val count = item.size
          println(s"$count duplicates:\n\t" + (item mkString "\n\t") + "\n")
        }
    }
  }

}
