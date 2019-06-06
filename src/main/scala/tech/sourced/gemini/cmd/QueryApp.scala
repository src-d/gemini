package tech.sourced.gemini.cmd

import com.datastax.driver.core.Cluster
import io.grpc.ManagedChannelBuilder
import org.bblfsh.client.BblfshClient
import tech.sourced.featurext.generated.service.FeatureExtractorGrpc
import tech.sourced.gemini.util.Logger
import tech.sourced.gemini.{Gemini, QueryResult}

case class QueryAppConfig(
  file: String = "",
  host: String = Gemini.defaultCassandraHost,
  port: Int = Gemini.defaultCassandraPort,
  keyspace: String = Gemini.defautKeyspace,
  bblfshHost: String = Gemini.defaultBblfshHost,
  bblfshPort: Int = Gemini.defaultBblfshPort,
  feHost: String = Gemini.defaultFeHost,
  fePort: Int = Gemini.defaultFePort,
  docFreqFile: String = "",
  verbose: Boolean = false,
  mode: String = Gemini.fileSimilarityMode
)

/**
  * Scala app that searches all hashed repos for a given file.
  */
object QueryApp extends App {
  val parser = new Parser[QueryAppConfig]("./query") {
    head("Gemini Query")
    note("Finds duplicate file among hashed repositories")

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
    opt[Unit]('v', "verbose")
      .action((_, c) => c.copy(verbose = true))
      .text("producing more verbose debug output")
    opt[String]("doc-freq-file")
      .action((x, c) => c.copy(docFreqFile = x))
      .text("path to file with feature frequencies")
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
    arg[String]("<path-to-file>")
      .required()
      .action((x, c) => c.copy(file = x))
      .text("path to a file to query")
  }

  parser.parseWithEnv(args, QueryAppConfig()) match {
    case Some(config) =>
      val log = Logger("gemini", config.verbose)

      val file = config.file
      println(s"Query all files similar to: $file")

      //TODO(bzz): wrap to CassandraConnector(config).withSessionDo { session =>
      log.info("Creating Cassandra connection")
      val cluster = Cluster.builder()
        .addContactPoint(config.host)
        .withPort(config.port)
        .build()
      val cassandra = cluster.connect()
      val gemini = Gemini(null, log, config.keyspace)
      log.info("Checking schema")
      gemini.applySchema(cassandra)

      log.info("Setting up bblfsh/fe gRPC clients")
      val bblfshClient = BblfshClient(config.bblfshHost, config.bblfshPort)
      val channel = ManagedChannelBuilder.forAddress(config.feHost, config.fePort).usePlaintext(true).build()
      val feClient = FeatureExtractorGrpc.stub(channel)
      val QueryResult(duplicates, similar) =
        gemini.query(
          file,
          cassandra,
          bblfshClient,
          config.mode,
          config.docFreqFile,
          feClient
        )

      cassandra.close()
      cluster.close()

      if (duplicates.isEmpty) {
        println(s"No duplicates of $file found.")
      } else {
        println(s"Duplicates of $file:\n\t" + (duplicates mkString "\n\t"))
      }

      if (similar.isEmpty) {
        println(s"No similar ${config.mode}s for $file found.")
      } else {
        println(s"Similar ${config.mode}s of $file:\n\t" + (similar mkString "\n\t"))
      }

    case None =>
      System.exit(2)
  }
}
