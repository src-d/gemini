package tech.sourced.gemini

import com.datastax.driver.core.Cluster

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

  //wrap to CassandraConnector(config).withSessionDo { session =>
  val cluster = Cluster.builder().addContactPoint("0.0.0.0").build()
  val session = cluster.connect()
  val similar = Gemini.query(file, session)

  session.close
  cluster.close

  if (similar.isEmpty) {
    println(s"NodDuplicates of $file found.")
    System.exit(1)
  } else {
    println(s"Duplicates of $file:\n\t" + (similar mkString ("\n\t")))
  }
}
