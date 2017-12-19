package tech.sourced.gemini

import com.datastax.driver.core.Cluster

object ReportSparkApp extends App {
  def printUsage(): Unit = {
    println("Usage: ./report [--use-group-by|--condensed]")
    println("")
    println("Finds duplicate files among hashed repositories." +
      "\n\tIt uses as many queries as distinct files are stored in the database"
    )
    println("  --use-group-by" +
      "\n\tIt uses as many queries as unique duplicate files are found, plus one. (only for Apache Cassandra database)"
    )
    println("  --condensed displays only the duplicate blob_hash and the number of occurrences." +
      "\n\tIt uses only one query to find the duplicates. (only for Apache Cassandra database)"
    )
    System.exit(2)
  }

  def print(report: Report): Unit = {
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

  val ERROR = -1
  val DEFAULT = 0
  val CONDENSED = 1
  val GROUP_BY = 2

  val mode = args match {
    case _ if args.length > 1 => ERROR
    case _ if args.length == 0 => DEFAULT
    case _ if args.length == 1 && args(0) == "--use-group-by" => GROUP_BY
    case _ if args.length == 1 && args(0) == "--condensed" => CONDENSED
    case _ => ERROR
  }

  if (mode == ERROR) {
    printUsage()
  }

  //TODO(bzz): wrap to CassandraConnector(config).withSessionDo { session =>
  val cluster = Cluster.builder().addContactPoint(Gemini.defaultCassandraHost).build()
  val cassandra = cluster.connect()
  val gemini = Gemini(null)
  gemini.applySchema(cassandra)

  val report = mode match {
    case DEFAULT => gemini.report(cassandra)
    case CONDENSED => gemini.reportCassandraCondensed(cassandra)
    case GROUP_BY => gemini.reportCassandraGroupBy(cassandra)
  }

  cassandra.close
  cluster.close

  print(report)
}
