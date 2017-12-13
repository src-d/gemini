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

  def print(report: Iterable[Any], detailed: Boolean): Unit = {
    if (report.isEmpty) {
      println(s"No duplicates found.")
    } else if (detailed) {
      report.foreach { item =>
        val duplicateFiles = item.asInstanceOf[Iterable[RepoFile]]
        val count = duplicateFiles.count(_ => true)
        println(s"$count duplicates:\n\t" + (duplicateFiles mkString "\n\t") + "\n")
      }
    } else {
      println(s"Duplicates found:\n\t" + (report mkString "\n\t"))
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

  val cluster = Cluster.builder().addContactPoint(Gemini.defaultCassandraHost).build()
  val session = cluster.connect()

  val report = Gemini.report(session, detailed)
  print(report, detailed)

  session.close
  cluster.close
}
