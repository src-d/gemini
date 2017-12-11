package tech.sourced.gemini

import java.io.{File, FileInputStream}

import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.eclipse.jgit.lib.Constants.OBJ_BLOB
import org.eclipse.jgit.lib.ObjectInserter
import tech.sourced.engine._

import scala.collection.JavaConverters._
import scala.io.Source


class Gemini(session: SparkSession) {

  def hash(reposPath: String): DataFrame = {
    val engine = Engine(session, reposPath)

    val headRefs = engine.getRepositories.getHEAD.withColumnRenamed("hash", "commit_hash")
    val files = headRefs.getCommits.getFirstReferenceCommit.getFiles.select("file_hash", "commit_hash", "path")

    val filesInRepos = files.join(headRefs, "commit_hash")

    val filesToWrite = filesInRepos
      .select("file_hash", "path", "repository_id")
      .withColumnRenamed("file_hash", "blob_hash")
      .withColumnRenamed("repository_id", "repo")
      .withColumnRenamed("path", "file_path")

    filesToWrite
  }

  def hashAndSave(reposPath: String): Unit = {
    val files = hash(reposPath)
    println(s"Writing ${files.rdd.countApprox(10000L)} files to DB")
    files.write
      .mode("append")
      .cassandraFormat("blob_hash_files", "hashes")
      .save()
  }

}

case class RepoFile(repo: String, file: String, sha: String)

object Gemini {
  val defaultCassandraHost: String = "127.0.0.1"
  val defaultCassandraPort: String = "9042"
  val defaultSchemaFile: String = "src/main/resources/schema.cql"

  val formatter = new ObjectInserter.Formatter

  def apply(ss: SparkSession): Gemini = new Gemini(ss)

  /**
    * Search for duplicates and similar items to the given one.
    *
    * @param inPath path to an item
    * @param conn   Database connection
    * @return
    */
  def query(inPath: String, conn: Session): Iterable[RepoFile] = {
    val path = new File(inPath)
    if (path.isDirectory) {
      findDuplicateProjects(path, conn)
      //TODO: implement based on Apolo
      //findSimilarProjects(path, conn)
    } else {
      findDuplicateFiles(path, conn)
      //TODO: implement based on Apolo
      //findSimilarFiles(path, conn)
    }
  }

  /**
    * Finds duplicated files among hashed repositories that already exists in certain repository
    *
    * @param repository repository url, example: github.com/src-d/go-git.git"
    * @return
    */
  def report(repository: String): Iterable[RepoFile] = {
    throw new UnsupportedOperationException("Finding all duplicate files in many repositories is no implemented yet.")
  }

  def findDuplicateProjects(in: File, conn: Session): Iterable[RepoFile] = {
    //TODO(bzz): project is duplicate if it has all it's files in some other projects
    throw new UnsupportedOperationException("Finding duplicate repositories is no implemented yet.")
  }

  def findDuplicateFiles(file: File, conn: Session): Iterable[RepoFile] = {
    val sha = computeSha1(file)

    val query = QueryBuilder.select().all().from("hashes", "blob_hash_files").where(QueryBuilder.eq("blob_hash", sha))
    var results: Iterable[RepoFile] = Iterable()

    results = conn
      .execute(query)
      .asScala map { row =>
        RepoFile(row.getString("repo"), row.getString("file_path"), row.getString("blob_hash"))
      }
    results
  }

  def computeSha1(file: File): String = {
    val in = new FileInputStream(file)
    val objectId = formatter.idFor(OBJ_BLOB, file.length, in)
    in.close()
    objectId.getName
  }

  def applySchema(session: Session, pathToCqlFile: String = defaultSchemaFile): Unit = {
    println("CQL: creating schema") //TODO(bzz): Log.Debug
    Source
      .fromFile(pathToCqlFile)
      .getLines
      .map(_.trim)
      .filter(!_.isEmpty)
      .foreach { line =>
        println(s"CQL: $line")
        session.execute(line)
      }
    println("CQL: Done. Schema created")
  }

}
