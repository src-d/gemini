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

class Gemini(session: SparkSession, keyspace: String = Gemini.defautKeyspace) {

  import session.implicits._

  def hash(reposPath: String, limit: Int = 0): DataFrame = {
    if (session == null) {
      throw new UnsupportedOperationException("Hashing requires a SparkSession.")
    }

    val engine = Engine(session, reposPath)

    // engine.getRepositories.limit(n)...getFiles - doesn't work in engine now
    // https://github.com/src-d/engine/issues/267
    // use workaround with filter
    if (limit <= 0) {
      hashForRepos(engine.getRepositories)
    } else {
      val repoIds = engine.getRepositories.limit(limit).select($"id").collect().map(_ (0))
      hashForRepos(engine.getRepositories.filter($"id".isin(repoIds: _*)))
    }
  }

  def hashForRepos(repos: DataFrame): DataFrame = {
    val headRefs = repos
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
      .cassandraFormat("blob_hash_files", keyspace)
      .save()
  }

  def hashAndSave(reposPath: String, limit: Int = 0): Unit = {
    val files = hash(reposPath, limit)
    save(files)
  }

  /**
    * Search for duplicates and similar items to the given one.
    *
    * @param inPath path to an item
    * @param conn   Database connection
    * @return
    */
  def query(inPath: String, conn: Session): ReportByLine = {
    val path = new File(inPath)
    if (path.isDirectory) {
      ReportByLine(Gemini.findDuplicateProjects(path, conn, keyspace))
      //TODO: implement based on Apolo
      //findSimilarProjects(path)
    } else {
      ReportByLine(Gemini.findDuplicateItemForFile(path, conn, keyspace))
      //TODO: implement based on Apolo
      //findSimilarFiles(path)
    }
  }

  /**
    * Finds duplicate files among hashed repositories
    * It is used one query per distinct file
    *
    * @param conn Database connections
    * @return
    */
  def report(conn: Session): ReportExpandedGroup = {
    ReportExpandedGroup(Gemini.findAllDuplicateItems(conn, keyspace))
  }

  /**
    * Finds duplicate files among hashed repositories
    * It is used only one query
    * (Only supported by Apache Cassandra databases)
    *
    * @param conn Database connections
    * @return
    */
  def reportCassandraCondensed(conn: Session): ReportGrouped = {
    ReportGrouped(Gemini.findAllDuplicateBlobHashes(conn, keyspace))
  }

  /**
    * Finds duplicate files among hashed repositories
    * It is used one query per unique duplicate file, plus an extra one
    * (Only supported by Apache Cassandra databases)
    *
    * @param conn Database connections
    * @return
    */
  def reportCassandraGroupBy(conn: Session): ReportExpandedGroup = {
    val duplicates = reportCassandraCondensed(conn).v
      .map { item =>
        Gemini.findDuplicateItemForBlobHash(item.sha, conn, keyspace)
      }
    ReportExpandedGroup(duplicates)
  }

  def applySchema(session: Session): Unit = {
    println("CQL: creating schema") //TODO(bzz): Log.Debug
    Source
      .fromFile(Gemini.defaultSchemaFile)
      .getLines
      .map(_.trim)
      .filter(!_.isEmpty)
      .foreach { line =>
        val cql = line.replace("__KEYSPACE__", keyspace)
        println(s"CQL: $cql")
        session.execute(cql)
      }
    println("CQL: Done. Schema created")
  }

  def dropSchema(session: Session): Unit = {
    println("CQL: dropping schema") //TODO(bzz): Log.Debug
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
  }
}

case class RepoFile(repo: String, file: String, sha: String)

case class DuplicateBlobHash(sha: String, count: Long) {
  override def toString(): String = s"$sha ($count duplicates)"
}

object Gemini {

  val defaultCassandraHost: String = "127.0.0.1"
  val defaultCassandraPort: Int = 9042
  val defaultSchemaFile: String = "src/main/resources/schema.cql"
  val defautKeyspace: String = "hashes"

  val formatter = new ObjectInserter.Formatter

  def apply(ss: SparkSession, keyspace: String = defautKeyspace): Gemini = new Gemini(ss, keyspace)

  def computeSha1(file: File): String = {
    val in = new FileInputStream(file)
    val objectId = formatter.idFor(OBJ_BLOB, file.length, in)
    in.close()
    objectId.getName
  }

  /**
    * Finds the blob_hash that are repeated in the database, and how many times
    * (Only supported by Apache Cassandra databases)
    *
    * @param conn     Database connections
    * @param keyspace Keyspace under data is stored
    * @return
    */
  def findAllDuplicateBlobHashes(conn: Session, keyspace: String): Iterable[DuplicateBlobHash] = {
    val duplicatesCountCql = s"SELECT blob_hash, COUNT(*) as count FROM ${keyspace}.blob_hash_files GROUP BY blob_hash"
    conn
      .execute(new SimpleStatement(duplicatesCountCql))
      .asScala
      .filter(_.getLong("count") > 1)
      .map { r =>
        DuplicateBlobHash(r.getString("blob_hash"), r.getLong("count"))
      }
  }

  /**
    * Finds the groups of duplicate files identified by the blob_hash
    *
    * @param conn     Database connections
    * @param keyspace Keyspace under data is stored
    * @return
    */
  def findAllDuplicateItems(conn: Session, keyspace: String): Iterable[Iterable[RepoFile]] = {
    val distinctBlobHash = s"SELECT distinct blob_hash FROM ${keyspace}.blob_hash_files"
    conn
      .execute(new SimpleStatement(distinctBlobHash))
      .asScala
      .flatMap { r =>
        val dupes = findDuplicateItemForBlobHash(r.getString("blob_hash"), conn, keyspace)
        if (dupes.size > 1) {
          List(dupes)
        } else {
          List()
        }
      }
  }

  def findDuplicateProjects(in: File, conn: Session, keyspace: String): Iterable[RepoFile] = {
    //TODO(bzz): project is duplicate if it has all it's files in some other projects
    throw new UnsupportedOperationException("Finding duplicate repositories is no implemented yet.")
  }

  def findDuplicateItemForFile(file: File, conn: Session, keyspace: String): Iterable[RepoFile] = {
    findDuplicateItemForBlobHash(Gemini.computeSha1(file), conn, keyspace)
  }

  def findDuplicateItemForBlobHash(sha: String, conn: Session, keyspace: String): Iterable[RepoFile] = {
    val query = QueryBuilder.select().all().from(keyspace, "blob_hash_files")
      .where(QueryBuilder.eq("blob_hash", sha))

    conn.execute(query).asScala.map { row =>
      RepoFile(row.getString("repo"), row.getString("file_path"), row.getString("blob_hash"))
    }
  }

}

sealed abstract class Report(v: Iterable[Any]) {
  def empty(): Boolean = {
    v.isEmpty
  }

  def size(): Int = v.size
}

case class ReportByLine(v: Iterable[RepoFile]) extends Report(v)

case class ReportGrouped(v: Iterable[DuplicateBlobHash]) extends Report(v)

case class ReportExpandedGroup(v: Iterable[Iterable[RepoFile]]) extends Report(v)
