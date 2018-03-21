package tech.sourced.gemini

import java.io.{File, FileInputStream}
import java.nio.file.Files

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Session, SimpleStatement}
import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bblfsh.client.BblfshClient
import org.eclipse.jgit.lib.Constants.OBJ_BLOB
import org.eclipse.jgit.lib.ObjectInserter
import org.slf4j.{Logger => Slf4jLogger}
import tech.sourced.engine._

import scala.collection.JavaConverters._
import scala.io.Source

class Gemini(session: SparkSession, log: Slf4jLogger, keyspace: String = Gemini.defautKeyspace) {

  import Gemini._
  import session.implicits._

  def hash(reposPath: String, limit: Int = 0, format: String = "siva"): DataFrame = {
    if (session == null) {
      throw new UnsupportedOperationException("Hashing requires a SparkSession.")
    }

    val engine = Engine(session, reposPath, format)

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

  def hashForRepos(repos: DataFrame): DataFrame =
    repos
      .getHEAD
      .getCommits
      .getTreeEntries
      .getBlobs
      .select("blob_id", "repository_id", "commit_hash", "path")
      .withColumnRenamed("blob_id", meta.sha)
      .withColumnRenamed("repository_id", meta.repo)
      .withColumnRenamed("commit_hash", meta.commit)
      .withColumnRenamed("path", meta.path)

  def save(files: DataFrame): Unit = {
    val approxFileCount = files.rdd.countApprox(10000L)
    log.info(s"Writing $approxFileCount files to DB")
    files.write
      .mode("append")
      .cassandraFormat(defaultTable, keyspace)
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
  def query(inPath: String, conn: Session, bblfshClient: BblfshClient): Iterable[RepoFile] = {
    val path = new File(inPath)
    if (path.isDirectory) {
      findDuplicateProjects(path, conn, keyspace)
      //TODO: implement based on Apollo
      //findSimilarProjects(path)
    } else {
      findSimilarForFile(path, bblfshClient)
      findDuplicateItemForFile(path, conn, keyspace)
    }
  }

  // TODO: should return something later
  def findSimilarForFile(file: File, client: BblfshClient): Unit = {
    val uast = extractUAST(file, client)
    if (uast.isDefined) {
      log.info(s"uast received: ${uast.toString}")
      // TODO: extract features, calculate minhash, find similar
    }
  }

  def extractUAST(file: File, client: BblfshClient): Option[Node] = {
    val byteArray = Files.readAllBytes(file.toPath)

    val resp = client.parse(file.getName, new String(byteArray))
    if (resp.errors.nonEmpty) {
      val errors = resp.errors.mkString(",")
      log.error(s"bblfsh errors: ${errors}")
    }

    resp.uast
  }

  /**
    * Finds duplicate files among hashed repositories
    * It is used one query per distinct file
    *
    * @param conn Database connections
    * @return
    */
  def report(conn: Session): Iterable[Iterable[RepoFile]] = {
    findAllDuplicateItems(conn, keyspace)
  }

  /**
    * Finds duplicate files among hashed repositories
    * It is used only one query
    * (Only supported by Apache Cassandra databases)
    *
    * @param conn Database connections
    * @return
    */
  def reportCassandraCondensed(conn: Session): Iterable[DuplicateBlobHash] = {
    findAllDuplicateBlobHashes(conn, keyspace)
  }

  /**
    * Finds duplicate files among hashed repositories
    * It is used one query per unique duplicate file, plus an extra one
    * (Only supported by Apache Cassandra databases)
    *
    * @param conn Database connections
    * @return
    */
  def reportCassandraGroupBy(conn: Session): Iterable[Iterable[RepoFile]] = {
    reportCassandraCondensed(conn)
      .map { item =>
        findDuplicateItemForBlobHash(item.sha, conn, keyspace)
      }
  }

  def findConnectedComponents(conn: Session): Map[Int, Set[Int]] = {
    val cc = new DBConnectedComponents(log, conn, "hashtables", keyspace)
    val buckets = cc.makeBuckets()
    val elsToBuckets = cc.elementsToBuckets(buckets)

    cc.findInBuckets(buckets, elsToBuckets)
  }

  def applySchema(session: Session): Unit = {
    log.debug("CQL: creating schema")
    Source
      .fromFile(defaultSchemaFile)
      .getLines
      .map(_.trim)
      .filter(!_.isEmpty)
      .foreach { line =>
        val cql = line.replace("__KEYSPACE__", keyspace)
        log.debug(s"CQL: $cql")
        session.execute(cql)
      }
    log.debug("CQL: Done. Schema created")
  }

  def dropSchema(session: Session): Unit = {
    log.debug("CQL: dropping schema")
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
  }
}

object URLFormatter {
  private val services = Map(
    "github.com" -> "https://%s/blob/%s/%s",
    "bitbucket.org" -> "https://%s/src/%s/%s",
    "gitlab.com" -> "https://%s/blob/%s/%s"
  )
  private val default = ("", "repo: %s commit: %s path: %s")

  def format(repo: String, commit: String, path: String): String = {
    val urlTemplateByRepo = services.find { case (h, _) => repo.startsWith(h) }.getOrElse(default)._2
    val repoWithoutSuffix = repo.replaceFirst("\\.git$", "")

    urlTemplateByRepo.format(repoWithoutSuffix, commit, path)
  }
}

case class Meta(sha: String, repo: String, commit: String, path: String)

case class RepoFile(repo: String, commit: String, path: String, sha: String) {
  override def toString: String = URLFormatter.format(repo, commit, path)
}

case class DuplicateBlobHash(sha: String, count: Long) {
  override def toString: String = s"$sha ($count duplicates)"
}

object Gemini {

  val defaultCassandraHost: String = "127.0.0.1"
  val defaultCassandraPort: Int = 9042
  val defaultSchemaFile: String = "src/main/resources/schema.cql"
  val defautKeyspace: String = "hashes"
  val defaultTable: String = "blob_hash_files"
  val defaultBblfshHost: String = "127.0.0.1"
  val defaultBblfshPort: Int = 9432

  //TODO(bzz): switch to `tables("meta")`
  val meta = Meta("sha1", "repo", "commit", "path")

  val formatter = new ObjectInserter.Formatter

  def apply(ss: SparkSession, log: Slf4jLogger = Logger("gemini"), keyspace: String = defautKeyspace): Gemini =
    new Gemini(ss, log, keyspace)

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
    val hash = meta.sha
    val dupCount = "count"
    val duplicatesCountCql = s"SELECT $hash, COUNT(*) as $dupCount FROM $keyspace.$defaultTable GROUP BY $hash"
    conn
      .execute(new SimpleStatement(duplicatesCountCql))
      .asScala
      .filter(_.getLong(dupCount) > 1)
      .map { r =>
        DuplicateBlobHash(r.getString(meta.sha), r.getLong(dupCount))
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
    val hash = meta.sha
    val distinctBlobHash = s"SELECT distinct $hash FROM $keyspace.$defaultTable"
    conn
      .execute(new SimpleStatement(distinctBlobHash))
      .asScala
      .flatMap { r =>
        val dupes = findDuplicateItemForBlobHash(r.getString(hash), conn, keyspace)
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
    findDuplicateItemForBlobHash(computeSha1(file), conn, keyspace)
  }

  def findDuplicateItemForBlobHash(sha: String, conn: Session, keyspace: String): Iterable[RepoFile] = {
    val query = QueryBuilder.select().all().from(keyspace, defaultTable)
      .where(QueryBuilder.eq(meta.sha, sha))

    conn.execute(query).asScala.map { row =>
      RepoFile(row.getString(meta.repo), row.getString(meta.commit), row.getString(meta.path), row.getString(meta.sha))
    }
  }
}

