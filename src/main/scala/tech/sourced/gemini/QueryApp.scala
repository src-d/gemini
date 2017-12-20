package tech.sourced.gemini

import com.datastax.driver.core.Cluster

case class QueryAppConfig(file: String = "",
                          host: String = Gemini.defaultCassandraHost,
                          port: Int = Gemini.defaultCassandraPort)

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
    arg[String]("<path-to-file>")
      .required()
      .action((x, c) => c.copy(file = x))
      .text("path to a file to query")
  }

  parser.parse(args, QueryAppConfig()) match {
    case Some(config) =>
      val file = config.file
      println(s"Query duplicate files to: $file")

      //TODO(bzz): wrap to CassandraConnector(config).withSessionDo { session =>
      val cluster = Cluster.builder()
        .addContactPoint(config.host)
        .withPort(config.port)
        .build()
      val cassandra = cluster.connect()
      val gemini = Gemini(null)
      gemini.applySchema(cassandra)

      val similar = gemini.query(file, cassandra).v

      cassandra.close
      cluster.close

      if (similar.isEmpty) {
        println(s"No duplicates of $file found.")
        System.exit(1)
      } else {
        println(s"Duplicates of $file:\n\t" + (similar mkString ("\n\t")))
      }

    case None =>
      System.exit(2)
  }
}
