package tech.sourced.gemini

import com.datastax.driver.core.Cluster

object ReportSparkApp extends App {
  def printUsage(): Unit = {
    println("Usage: ./report [--detailed]")
    println("")
    println("Finds duplicate files among hashed repositories")
    println("  --detailed to see file paths of each duplicated file")
    System.exit(2)
  }

  def print(report: Report, detailed: Boolean): Unit = {
    report match {
      case e if e.empty() => println(s"No duplicates found.")
      case r: ReportGrouped => println(s"Duplicates found:\n\t" + (r.v mkString "\n\t"))
      case g: ReportExpandedGroup => {
        g.v.foreach { item =>
          val count = item.size
          println(s"$count duplicates:\n\t" + (item mkString "\n\t") + "\n")
        }
      }
    }
  }

  var detailed = false
  if (args.length > 0) {
    val arg = args(0)
    if (arg == "--detailed") {
      detailed = true
    } else {
      printUsage()
    }
  }

  //TODO(bzz): wrap to CassandraConnector(config).withSessionDo { session =>
  val cluster = Cluster.builder().addContactPoint(Gemini.defaultCassandraHost).build()
  val cassandra = cluster.connect()
  val gemini = Gemini(null)
  gemini.applySchema(cassandra)

  val report = gemini.report(detailed, cassandra)

  cassandra.close
  cluster.close

  print(report, detailed)
}
