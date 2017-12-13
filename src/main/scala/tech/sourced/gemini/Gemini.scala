package tech.sourced.gemini

import java.io.{File, FileInputStream}

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Session, SimpleStatement}
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

    val headRefs = engine.getRepositories
      .getReferences //TODO(bzz) replace \w .getHead() after https://github.com/src-d/engine/issues/255
      .filter("name = 'refs/heads/HEAD' OR name = 'HEAD'")
      .withColumnRenamed("hash", "commit_hash")
    val files = headRefs.getCommits.getFirstReferenceCommit.getFiles.select("file_hash", "commit_hash", "path")

    val filesInRepos = files.join(headRefs, "commit_hash")

    val filesToWrite = filesInRepos
      .select("file_hash", "path", "repository_id")
      .withColumnRenamed("file_hash", "blob_hash")
      .withColumnRenamed("repository_id", "repo")
      .withColumnRenamed("path", "file_path")

    filesToWrite
  }

  def save(files: DataFrame): Unit = {
    println(s"Writing ${files.rdd.countApprox(10000L)} files to DB")
    files.write
      .mode("append")
      .cassandraFormat("blob_hash_files", "hashes")
      .save()
  }

  def hashAndSave(reposPath: String): Unit = {
    val files = hash(reposPath)
    save(files)
  }

}

case class RepoFile(repo: String, file: String, sha: String)
case class DuplicateBlobHash(sha: String, count: Long) {
   override def toString(): String = s"$sha ($count duplicates)"
}

object Gemini {
  val defaultCassandraHost: String = "127.0.0.1"
  val defaultCassandraPort: String = "9042"
  val defaultSchemaFile: String = "src/main/resources/schema.cql"

  val cql = "SELECT blob_hash, repo, file_path, COUNT(*) as count FROM hashes.blob_hash_files GROUP BY blob_hash"

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
      findDuplicateItemForFile(path, conn)
      //TODO: implement based on Apolo
      //findSimilarFiles(path, conn)
    }
  }

  /**
    * Finds duplicate files among hashed repositories
    *
    * @param conn Database connection
    * @return
    */
  def report(conn: Session, detailed: Boolean): Iterable[Any] = {
    val duplicates = findAllDuplicateItems(conn)
    if (detailed) {
      duplicates map { item =>
        findDuplicateItemForBlobHash(item.sha, conn)
      }
    } else {
      duplicates
    }
  }

  /**
    * Finds duplicate files among hashed repositories, and returns all duplicated files
    *
    * @param conn   Database connection
    * @return
    */
  def findAllDuplicateItems(conn: Session): Iterable[DuplicateBlobHash] = {
    conn
      .execute(new SimpleStatement(cql))
      .asScala
      .filter( _.getLong("count") > 1 )
      .map { r => DuplicateBlobHash(r.getString("blob_hash"), r.getLong("count")) }
  }

  def findDuplicateProjects(in: File, conn: Session): Iterable[RepoFile] = {
    //TODO(bzz): project is duplicate if it has all it's files in some other projects
    throw new UnsupportedOperationException("Finding duplicate repositories is no implemented yet.")
  }

  def findDuplicateItemForFile(file: File, conn: Session): Iterable[RepoFile] = {
    findDuplicateItemForBlobHash(computeSha1(file), conn)
  }

  def findDuplicateItemForBlobHash(sha: String, conn: Session): Iterable[RepoFile] = {
    val query = QueryBuilder.select().all().from("hashes", "blob_hash_files").where(QueryBuilder.eq("blob_hash", sha))
    conn.execute(query).asScala.map { row =>
      RepoFile(row.getString("repo"), row.getString("file_path"), row.getString("blob_hash"))
    }
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
