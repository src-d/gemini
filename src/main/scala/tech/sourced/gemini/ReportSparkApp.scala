package tech.sourced.gemini

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session

object ReportSparkApp extends App {
  def printUsage(): Unit = {
    println("Usage: ./report [--detailed]")
    println("")
    println("Finds duplicate files among hashed repositories")
    println("  --detailed to see file paths of each duplicated file")
    System.exit(2)
  }

  def printIsEmpty[T](report:Iterable[T]): Unit = {
    if (report.isEmpty) {
      println(s"No duplicates found.")
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

  if (detailed) {
    val report = Gemini.detailedReport(session)
    printIsEmpty(report)
    report.foreach( duplicateFiles => {
      val count = duplicateFiles.count(_=>true)
      println (s"$count duplicates:\n\t" + (duplicateFiles mkString ("\n\t")))
    })
  } else {
    val report = Gemini.report(session)
    printIsEmpty(report)
    println(s"Duplicates found:\n\t" + (report mkString ("\n\t")))
  }

  session.close
  cluster.close
}
