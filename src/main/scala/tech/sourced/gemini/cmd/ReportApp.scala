package tech.sourced.gemini.cmd

import java.io.File
import java.util

import com.datastax.driver.core.{Cluster, Session}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.slf4j.{Logger => Slf4jLogger}
import tech.sourced.gemini.{DuplicateBlobHash, Gemini, Logger, RepoFile}

import scala.collection.JavaConverters._
import scala.sys.process._

case class ReportAppConfig(host: String = Gemini.defaultCassandraHost,
                           port: Int = Gemini.defaultCassandraPort,
                           keyspace: String = Gemini.defautKeyspace,
                           mode: String = ReportApp.defaultMode,
                           ccDirPath: String = ".",
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
      .action((x, c) => c.copy(ccDirPath = x))
      .text("directory path to output parquet files with connected components")
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

      val duplicateReport = config.mode match {
        case `defaultMode` => ReportExpandedGroup(gemini.report(cassandra))
        case `condensedMode` => ReportGrouped(gemini.reportCassandraCondensed(cassandra))
        case `groupByMode` => ReportExpandedGroup(gemini.reportCassandraGroupBy(cassandra))
      }
      val similarityReport = makeSimilarityReport(gemini, cassandra, config)

      print(duplicateReport)
      printCommunities(similarityReport)

      log.info("Closing DB connection")
      cassandra.close()
      cluster.close()

    case None =>
      System.exit(2)
  }

  def makeSimilarityReport(
                            gemini: Gemini,
                            cassandra: Session,
                            config: ReportAppConfig): Iterable[Iterable[RepoFile]] = {

    val log = Logger("gemini", config.verbose)

    val (connectedComponents, elsToBuckets, elementIds) = gemini.findConnectedComponents(cassandra)
    saveConnectedComponents(log, connectedComponents, elsToBuckets, config.ccDirPath)

    log.info("Detecting communities in Python")
    val pythonCmd = s"python3 src/main/python/community-detector/report.py ${config.ccDirPath}"
    val rc = pythonCmd.!

    if (rc == 0) {
      val communities = readCommunities(log, config.ccDirPath)
      gemini.reportCommunities(cassandra, communities, elementIds)
    } else {
      log.error(s"Failed to execute '${pythonCmd}', skipping similarity report")
      Iterable[Iterable[RepoFile]]()
    }
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

  def printCommunities(report: Iterable[Iterable[RepoFile]]): Unit = {
    if (report.isEmpty) {
      println(s"No similar files found.")
    } else {
      report.foreach { community =>
        val count = community.size
        println(s"$count similar files:\n\t${community.mkString("\n\t")}\n")
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

  def saveConnectedComponents(log: Slf4jLogger,
                              ccs: Map[Int, Set[Int]],
                              elsToBuckets: Map[Int, List[Int]],
                              outputPath: String): Unit = {
    log.info("Saving ConnectedComponents to Parquet")
    val schema = SchemaBuilder
      .record("ccs")
      .fields()
      .name("cc").`type`().intType().noDefault()
      .name("element_ids").`type`().array().items().intType().noDefault()
      .endRecord()

    // delete old file if exists
    val parquetFile = new File(s"$outputPath/cc.parquet")
    parquetFile.delete()

    // make it compatible with python
    val parquetConf = new Configuration()
    parquetConf.setBoolean("parquet.avro.write-old-list-structure", false)

    val parquetFilePath = new Path(s"$outputPath/cc.parquet")
    val writer = AvroParquetWriter.builder[GenericRecord](parquetFilePath)
      .withSchema(schema)
      .withConf(parquetConf)
      .build()

    ccs.foreach { case (cc, ids) =>
      val record = new GenericRecordBuilder(schema)
        .set("cc", cc)
        .set("element_ids", ids.toArray)
        .build()

      writer.write(record)
    }

    writer.close()

    log.info("Saving auxiliary data structure (id to buckets) to Parquet")
    val schemaBuckets = SchemaBuilder
      .record("id_to_buckets")
      .fields()
      .name("buckets").`type`().array().items().intType().noDefault()
      .endRecord()

    // delete old file if exists
    val parquetFileButckets = new File(s"$outputPath/buckets.parquet")
    parquetFileButckets.delete()

    val parquetBucketsFilePath = new Path(s"$outputPath/buckets.parquet")
    val writerBuckets = AvroParquetWriter.builder[GenericRecord](parquetBucketsFilePath)
      .withSchema(schemaBuckets)
      .withConf(parquetConf)
      .build()

    elsToBuckets.foreach { case (_, bucket) =>
      val record = new GenericRecordBuilder(schemaBuckets)
        .set("buckets", bucket.toArray)
        .build()

      writerBuckets.write(record)
    }

    writerBuckets.close()
  }

  def readCommunities(log: Slf4jLogger, dirPath: String): List[(Int, List[Int])] = {
    val parquetFilePath = new Path(s"$dirPath/communities.parquet")
    log.info(s"Reading detected communities from $parquetFilePath")
    val reader = AvroParquetReader.builder[GenericRecord](parquetFilePath).build()

    Iterator
      .continually(reader.read)
      .takeWhile(_ != null)
      .map { record =>
        val communityId = record.get("community_id").asInstanceOf[Long].toInt

        val elementIds = record
          .get("element_ids")
          .asInstanceOf[util.ArrayList[GenericData.Record]]
          .asScala
          .toList
          .map(_.get("item").asInstanceOf[Long].toInt)

        (communityId, elementIds)
      }
      .toList
  }
}
