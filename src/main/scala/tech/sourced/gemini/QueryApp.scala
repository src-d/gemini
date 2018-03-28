package tech.sourced.gemini

import com.datastax.driver.core.Cluster
import io.grpc.ManagedChannelBuilder
import org.bblfsh.client.BblfshClient
import tech.sourced.featurext.generated.service.FeatureExtractorGrpc

case class QueryAppConfig(file: String = "",
                          host: String = Gemini.defaultCassandraHost,
                          port: Int = Gemini.defaultCassandraPort,
                          bblfshHost: String = Gemini.defaultBblfshHost,
                          bblfshPort: Int = Gemini.defaultBblfshPort,
                          feHost: String = Gemini.defaultFeHost,
                          fePort: Int = Gemini.defaultFePort,
                          verbose: Boolean = false)

/**
  * Scala app that searches all hashed repos for a given file.
  */
object QueryApp extends App {
  val parser = new scopt.OptionParser[QueryAppConfig]("./query") {
    head("Gemini Query")
    note("Finds duplicate file among hashed repositories")

    opt[String]('h', "host")
      .action((x, c) => c.copy(host = x))
      .text("host is Cassandra host")
    opt[Int]('p', "port")
      .action((x, c) => c.copy(port = x))
      .text("port is Cassandra port")
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
      .action((x, c) => c.copy(bblfshPort = x))
      .text("port is features-extractor server port")
    opt[Unit]('v', "verbose")
      .action((_, c) => c.copy(verbose = true))
      .text("producing more verbose debug output")
    arg[String]("<path-to-file>")
      .required()
      .action((x, c) => c.copy(file = x))
      .text("path to a file to query")
  }

  parser.parse(args, QueryAppConfig()) match {
    case Some(config) =>
      val log = Logger("gemini", config.verbose)

      val file = config.file
      println(s"Query duplicate files to: $file")

      //TODO(bzz): wrap to CassandraConnector(config).withSessionDo { session =>
      val cluster = Cluster.builder()
        .addContactPoint(config.host)
        .withPort(config.port)
        .build()
      val cassandra = cluster.connect()
      val gemini = Gemini(null, log)
      gemini.applySchema(cassandra)

      val bblfshClient = BblfshClient.apply(config.bblfshHost, config.bblfshPort)
      val channel = ManagedChannelBuilder.forAddress(config.feHost, config.fePort).usePlaintext(true).build()
      val feClient = FeatureExtractorGrpc.stub(channel)
      val similar = gemini.query(file, cassandra, bblfshClient, feClient)

      cassandra.close()
      cluster.close()

      if (similar.isEmpty) {
        println(s"No duplicates of $file found.")
        System.exit(1)
      } else {
        println(s"Duplicates of $file:\n\t" + (similar mkString "\n\t"))
      }

    case None =>
      System.exit(2)
  }
}
