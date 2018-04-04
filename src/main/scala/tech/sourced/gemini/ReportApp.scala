package tech.sourced.gemini

import java.io.File

import org.apache.hadoop.fs.Path
import com.datastax.driver.core.Cluster
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter

case class ReportAppConfig(host: String = Gemini.defaultCassandraHost,
                           port: Int = Gemini.defaultCassandraPort,
                           keyspace: String = Gemini.defautKeyspace,
                           mode: String = ReportApp.defaultMode,
                           ccFilePath: String = "cc.parquet",
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
    opt[String]('k', "keyspace")
      .action((x, c) => c.copy(keyspace = x))
      .text("keyspace is Cassandra keyspace")
    opt[String]('o', "cc-output")
      .action((x, c) => c.copy(ccFilePath = x))
      .text("path to output parquet file with connected components")
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
      val gemini = Gemini(null, log, config.keyspace)
      gemini.applySchema(cassandra)


      val report = config.mode match {
        case `defaultMode` => ReportExpandedGroup(gemini.report(cassandra))
        case `condensedMode` => ReportGrouped(gemini.reportCassandraCondensed(cassandra))
        case `groupByMode` => ReportExpandedGroup(gemini.reportCassandraGroupBy(cassandra))
      }

      val connectedComponents = gemini.findConnectedComponents(cassandra)
      saveConnectedComponents(connectedComponents, config.ccFilePath)

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

  sealed abstract class Report(v: Iterable[Any]) {
    def empty(): Boolean = {
      v.isEmpty
    }

    def size(): Int = v.size
  }

  case class ReportByLine(v: Iterable[RepoFile]) extends Report(v)

  case class ReportGrouped(v: Iterable[DuplicateBlobHash]) extends Report(v)

  case class ReportExpandedGroup(v: Iterable[Iterable[RepoFile]]) extends Report(v)

  def saveConnectedComponents(cc: Map[Int, Set[Int]], outputPath: String): Unit = {
    val schema = SchemaBuilder
      .record("cc")
      .fields()
      .name("elements").`type`().array().items().intType().noDefault()
      .endRecord()

    // delete old file if exists
    val parquetFile = new File(outputPath)
    parquetFile.delete()

    // make it compatible with python
    val parquetConf = new Configuration()
    parquetConf.setBoolean("parquet.avro.write-old-list-structure", false)

    val parquetFilePath = new Path(outputPath)
    val writer = AvroParquetWriter.builder[GenericRecord](parquetFilePath)
      .withSchema(schema)
      .withConf(parquetConf)
      .build()

    cc.foreach { case (_, elements) =>
      val record = new GenericRecordBuilder(schema)
        .set("elements", elements.toArray)
        .build()

      writer.write(record)
    }

    writer.close()
  }

}
