package tech.sourced.gemini

import com.datastax.driver.core.Cluster

/**
  * Scala app that searches all hashed repos for a given file.
  */
object QueryApp extends App {
  def printUsage(): Unit = {
    println("Usage: ./query <path-to-file>")
    println("")
    println("Finds duplicate file among hashed repositories")
    println("  <path-to-file> - path to a file to query.")
    System.exit(2)
  }

  if (args.length <= 0) {
    printUsage()
  }

  val file = args(0)
  println(s"Query duplicate files to: $file")

  //TODO(bzz): wrap to CassandraConnector(config).withSessionDo { session =>
  val cluster = Cluster.builder().addContactPoint(Gemini.defaultCassandraHost).build()
  val cassandra = cluster.connect()
  val gemini = Gemini(null)
  gemini.applySchema(cassandra)

  val similar = gemini.query(file, cassandra)

  cassandra.close
  cluster.close

  if (similar.isEmpty) {
    println(s"No duplicates of $file found.")
    System.exit(1)
  } else {
    println(s"Duplicates of $file:\n\t" + (similar mkString ("\n\t")))
  }
}
