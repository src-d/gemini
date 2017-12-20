package tech.sourced.gemini

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import scala.util.Properties

/**
  * Apache Spark app that applied LSH to given repos, using source{d} Engine.
  */
object HashSparkApp extends App {
  def printUsage(): Unit = {
    println("Usage: ./hash <path-to-git-repos>")
    println("")
    println("Hashes given set of Git repositories, either from FS or as .siva files.")
    println("  <path-to-git-repos> - path to git repositories. Clones in local FS or Siva files in HDFS are supported.")
    System.exit(2)
  }

  if (args.length <= 0) {
    printUsage()
  }
  val reposPath = args(0)
  val spark = SparkSession.builder()
    .master(Properties.envOrElse("MASTER", "local[*]"))
    .getOrCreate()

  val repos = listRepositories(reposPath, spark.sparkContext.hadoopConfiguration)
  println(s"Hashing all ${repos.length} repositories in: $reposPath\n\t" + (repos mkString "\n\t"))

  val gemini = Gemini(spark, "hashes")
  CassandraConnector(spark.sparkContext).withSessionDo { cassandra =>
    gemini.applySchema(cassandra)
  }
  val filesToWrite = gemini.hash(reposPath)
  gemini.save(filesToWrite)
  println("Done")

  private def listRepositories(path: String, conf: Configuration): Array[Path] =
    FileSystem.get(conf)
      .listStatus(new Path(path))
      .filter { file =>
        file.isDirectory || file.getPath.getName.endsWith(".siva")
      }
      .map(_.getPath)

}
