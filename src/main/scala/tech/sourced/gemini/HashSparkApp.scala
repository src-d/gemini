package tech.sourced.gemini

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._

import scala.util.Properties


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
  val session = SparkSession.builder()
    .master(Properties.envOrElse("MASTER", "local[*]"))
    .getOrCreate()

  val repos = listRepositories(reposPath, session.sparkContext.hadoopConfiguration)
  println(s"Hashing all ${repos.length} repositories in: $reposPath\n\t" + (repos mkString ("\n\t")))

  val gemini = Gemini(session)
  val filesToWrite = gemini.hash(reposPath)

  println(s"Writing ${filesToWrite.rdd.countApprox(10000L)} files to DB")
  filesToWrite.write
    .mode("append")
    .cassandraFormat("blob_hash_files", "hashes")
    .save()
  println("Done")


  private def listRepositories(path: String, conf: Configuration): Array[Path] =
    FileSystem.get(conf)
      .listStatus(new Path(path))
      .filter { file =>
        file.isDirectory || file.getPath.getName.endsWith(".siva")
      }
      .map(_.getPath)

}
