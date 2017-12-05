package tech.sourced.gemini

import java.io.{File, FileInputStream}

import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.eclipse.jgit.lib.Constants.OBJ_BLOB
import org.eclipse.jgit.lib.ObjectInserter
import tech.sourced.engine._

import scala.collection.JavaConverters._
import scala.concurrent.Future
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

  //TODO
  def report(reposPath: String): DataFrame = {
    throw new UnsupportedOperationException("Finding all duplicate files in many repositories is no implemented yet.")
  }

}

case class RepoFile(repo: String, file: String)

object Gemini {
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

  //TODO
  def findDuplicateProjects(in: File, conn: Session): Iterable[RepoFile] = {
    throw new UnsupportedOperationException("Finding duplicate repositories is no implemented yet.")
  }

  def findDuplicateFiles(file: File, conn: Session): Iterable[RepoFile] = {
    val sha = computeSha1(file)

    val query = QueryBuilder.select().all().from("hashes", "blob_hash_files").where(QueryBuilder.eq("blob_hash", sha))
    var results: Iterable[RepoFile] = Iterable()

    results = conn
      .execute(query)
      .asScala map { row =>
        RepoFile(row.getString("repo"), row.getString("file_path"))
      }
    results
  }

  def computeSha1(file: File): String = {
    val in = new FileInputStream(file)
    val objectId = formatter.idFor(OBJ_BLOB, file.length, in)
    in.close()
    objectId.getName
  }

  def applySchema(session: Session, pathToCqlFile: String): TraversableOnce[Future[Any]] = {
    implicit val ec = scala.concurrent.ExecutionContext.global

    return Source
      .fromFile(pathToCqlFile)
      .getLines
      .map(_.trim)
      .filter(!_.isEmpty)
      .map { line =>
        println(s"CQL: $line")
        Future(session.execute(line))
      }
  }
}
