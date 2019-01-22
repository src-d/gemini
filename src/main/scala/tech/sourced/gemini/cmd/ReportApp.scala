package tech.sourced.gemini.cmd

import com.datastax.driver.core.Cluster
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import tech.sourced.gemini._
import tech.sourced.gemini.util.Logger


case class ReportAppConfig(
  host: String = Gemini.defaultCassandraHost,
  port: Int = Gemini.defaultCassandraPort,
  keyspace: String = Gemini.defautKeyspace,
  ccDirPath: String = ".",
  mode: String = Gemini.fileSimilarityMode,
  output: String = ReportApp.defaultOutput,
  cassandra: Boolean = false,
  verbose: Boolean = false
)

object ReportApp extends App {
  val outputText = "text"
  val outputJson = "json"
  val outputs = Array(outputText, outputJson)
  val defaultOutput = outputText

  val defaultFmt = ""
  val defaultFmtGroupBy = "use-group-by"

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
    opt[String]("output")
      .valueName(outputs.mkString(" | "))
      .validate(x =>
        if (outputs contains x) {
          success
        } else {
          failure(s"output must be one of: " + outputs.mkString(" | "))
        })
      .action((x, c) => c.copy(output = x))
      .text("output format")
    opt[Boolean]("cassandra")
      .action((x, c) => c.copy(cassandra = x))
      .text("Enable advanced cql queries for Apache Cassandra database")
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

      val result = gemini.report(cassandra, config.cassandra, config.ccDirPath)

      config.output match {
        case `outputText` => printAsText(result)
        case `outputJson` => printAsJson(result)
      }

      log.info("Closing DB connection")
      cassandra.close()
      cluster.close()

    case None =>
      System.exit(2)
  }

  def printAsText(result: ReportResult): Unit = {
    val ReportResult(duplicates, similarities) = result

    duplicates match {
      case e if e.empty() => println(s"No duplicated files found.")
      case ReportExpandedGroup(v) =>
        v.foreach { item =>
          val count = item.size
          println(s"$count duplicates:\n\t" + (item mkString "\n\t") + "\n")
        }
    }

    if (similarities.isEmpty) {
      println(s"No similarities found.")
    } else {
      similarities.foreach { community =>
        val count = community.size
        val typeName = community.head match {
          case SimilarFunc(_, _, _) => "functions"
          case _ => "files"
        }
        println(s"$count similar ${typeName}:\n\t${community.mkString("\n\t")}\n")
      }
    }
  }

  def printAsJson(result: ReportResult): Unit = {
    val ReportResult(duplicates, similarities) = result

    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)

    val str = mapper.writeValueAsString(Map(
      "duplicates" -> duplicates,
      "similarities" -> similarities
    ))
    println(str)
  }

}
