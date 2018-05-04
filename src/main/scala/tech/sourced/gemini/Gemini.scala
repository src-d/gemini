package tech.sourced.gemini

import java.io.{File, FileInputStream}

import com.datastax.driver.core.{Session, SimpleStatement}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bblfsh.client.BblfshClient
import org.eclipse.jgit.lib.Constants.OBJ_BLOB
import org.eclipse.jgit.lib.ObjectInserter
import org.slf4j.{Logger => Slf4jLogger}
import tech.sourced.engine.Engine
import tech.sourced.featurext.generated.service.FeatureExtractorGrpc.FeatureExtractor

import scala.collection.JavaConverters._
import scala.io.Source

class Gemini(session: SparkSession, log: Slf4jLogger, keyspace: String = Gemini.defautKeyspace) {

  import Gemini._
  import session.implicits._

  /**
    * Hash all files in reposPath
    *
    * @param reposPath
    * @param limit
    * @param format
    */
  def hash(reposPath: String, limit: Int = 0, format: String = "siva"): Unit = {
    if (session == null) {
      throw new UnsupportedOperationException("Hashing requires a SparkSession.")
    }

    // hash might be called more than one time with the same spark session
    // every run should re-process all repos/files
    session.catalog.clearCache()

    val hash = new Hash(session, log)
    val repos = getRepos(reposPath, limit, format)

    val result = hash.forRepos(repos)
    hash.save(result, keyspace, tables)
  }

  /**
    * Provides DataFrame with repositories by path with limit
    *
    * @param reposPath
    * @param limit
    * @param format
    * @return
    */
  def getRepos(reposPath: String, limit: Int = 0, format: String = "siva"): DataFrame = {
    val engine = Engine(session, reposPath, format)
    val repos = engine.getRepositories

    // engine.getRepositories.limit(n)...getFiles - doesn't work in engine now
    // https://github.com/src-d/engine/issues/267
    // use workaround with filter
    if (limit <= 0) {
      repos
    } else {
      val repoIds = repos.limit(limit).select($"id").collect().map(_ (0))
      repos.filter($"id".isin(repoIds: _*))
    }
  }

  /**
    * Search for duplicates and similar items to the given one.
    *
    * @param inPath path to an item
    * @param conn   Database connection
    * @return
    */
  def query(inPath: String,
            conn: Session,
            bblfshClient: BblfshClient,
            feClient: FeatureExtractor,
            docFreqPath: String = ""): QueryResult = {
    val path = new File(inPath)
    if (path.isDirectory) {
      QueryResult(findDuplicateProjects(path, conn, keyspace), findSimilarProjects(path))
    } else {
      val fileQuery = new FileQuery(conn, bblfshClient, feClient, docFreqPath, log, keyspace, tables)
      fileQuery.find(path)
    }
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
        Database.findFilesByHash(item.sha, conn, keyspace, tables)
      }
  }

  def reportCommunities(conn: Session,
                        communities: List[(Int, List[Int])],
                        elementIds: Map[String, Int]): Iterable[Iterable[RepoFile]] = {
    getCommunities(conn, keyspace, communities, elementIds)
  }

  /**
    * Return connected components from DB hashtables
    *
    * @param conn Database connections
    * @return - Map of connected components groupId to list of elements
    *         - Map of element ids to list of bucket indices
    *         - Map of element to ID
    */
  def findConnectedComponents(conn: Session): (Map[Int, Set[Int]], Map[Int, List[Int]], Map[String, Int]) = {
    val cc = new DBConnectedComponents(log, conn, tables.hashtables, keyspace)
    val (buckets, elementIds) = cc.makeBuckets()
    val elsToBuckets = cc.elementsToBuckets(buckets)

    val result = cc.findInBuckets(buckets, elsToBuckets)

    (result, elsToBuckets, elementIds)
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
  val defaultBblfshHost: String = "127.0.0.1"
  val defaultBblfshPort: Int = 9432
  val defaultFeHost: String = "127.0.0.1"
  val defaultFePort: Int = 9001
  val defaultDocFreqFile: String = "docfreq.json"

  val tables = Tables(
    "meta",
    "hashtables",
    MetaCols("sha1", "repo", "commit", "path"),
    HashtablesCols("sha1", "hashtable", "value")
  )

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
    val hash = tables.metaCols.sha
    val dupCount = "count"
    val duplicatesCountCql = s"SELECT $hash, COUNT(*) as $dupCount FROM $keyspace.${tables.meta} GROUP BY $hash"
    conn
      .execute(new SimpleStatement(duplicatesCountCql))
      .asScala
      .filter(_.getLong(dupCount) > 1)
      .map { r =>
        DuplicateBlobHash(r.getString(tables.metaCols.sha), r.getLong(dupCount))
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
    val hash = tables.metaCols.sha
    val distinctBlobHash = s"SELECT distinct $hash FROM $keyspace.${tables.meta}"

    conn
      .execute(new SimpleStatement(distinctBlobHash))
      .asScala
      .flatMap { r =>
        val dupes = Database.findFilesByHash(r.getString(hash), conn, keyspace, tables)
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

  def findSimilarProjects(in: File): Iterable[RepoFile] = {
    throw new UnsupportedOperationException("Finding similar repositories is no implemented yet.")
  }

  def getCommunities(conn: Session,
                     keyspace: String,
                     communities: List[(Int, List[Int])],
                     elementIds: Map[String, Int]): Iterable[Iterable[RepoFile]] = {

    val idToSha1 = for ((elem, id) <- elementIds) yield (id, elem.split("@")(1))

    // Transform communities of element IDs to communities of sha1s
    // Equivalent to apollo graph.py BatchedCommunityResolver._gen_hashes
    // https://github.com/src-d/apollo/blob/f51c5a92c24cbedd54b9b30bab02f03e51fd27b3/apollo/graph.py#L295
    val communitiesSha1 = communities
      .map { case (_, community) =>
        community
          .filter(idToSha1.contains)
          .map(idToSha1)
      }
      .filter(_.size > 1)


    communitiesSha1.map(sha1s => {
      val cols = tables.metaCols
      val elems = sha1s.map(st => s"'$st'").mkString(",")
      val query = s"select sha1, repo, commit, path from $keyspace.${tables.meta} where sha1 in ($elems)"

      conn
        .execute(new SimpleStatement(query))
        .asScala
        .map { row =>
          RepoFile(row.getString(cols.repo), row.getString(cols.commit),
            row.getString(cols.path), row.getString(cols.sha))
        }
    })
  }
}

