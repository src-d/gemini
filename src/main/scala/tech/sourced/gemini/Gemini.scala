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
import tech.sourced.gemini.cmd.ReportApp
import tech.sourced.gemini.util.{Logger, URLFormatter}

import scala.io.Source
import scala.util.matching.Regex

class Gemini(session: SparkSession, log: Slf4jLogger, keyspace: String = Gemini.defautKeyspace) {

  import Gemini._
  import session.implicits._

  /**
    * Hash all files in reposPath
    *
    * @param reposPath
    * @param limit
    * @param format repository input format
    * @param mode file or func similarity modes
    * @param docFreqPath (optional) path to DocFreq file. Default: read from DB.
    */
  def hash(reposPath: String, limit: Int = 0, format: String = "siva", mode: String, docFreqPath: String = ""): Unit = {
    if (session == null) {
      throw new UnsupportedOperationException("Hashing requires a SparkSession.")
    }
    log.warn(s"Getting repositories at $reposPath in $format format")

    // hash might be called more than one time with the same spark session
    // every run should re-process all repos/files
    session.catalog.clearCache()

    val hash = Hash(session, log, mode)
    val repos = getRepos(reposPath, limit, format)

    log.warn("Hashing")
    val result = hash.forRepos(repos)

    log.warn("Saving hashes to DB")
    hash.save(result, keyspace, tables, docFreqPath)
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
      log.info(s"Using only $limit repositories")
      val repoIds = repos.limit(limit).select($"id").collect().map(_ (0))
      repos.filter($"id".isin(repoIds: _*))
    }
  }

  val fileWithFuncPattern: Regex = "(.+):(.+):([0-9]+)".r

  /**
    * Search for duplicates and similar items to the given one.
    *
    * @param inPath path to an item in format path/to/file:func:line (func:line) are optional
    * @param conn   Database connection
    * @return
    */
  def query(inPath: String,
            conn: Session,
            bblfshClient: BblfshClient,
            mode: String,
            docFreqPath: String = "",
            feClient: FeatureExtractor): QueryResult = {
    log.info(s"Query for items similar to $inPath")
    val (path, fnFilter) = fileWithFuncPattern.findFirstMatchIn(inPath) match {
      case Some(m) => (new File(m.group(1)), Some(m.group(2), m.group(3).toInt))
      case None => (new File(inPath), None)
    }

    if (path.isDirectory) {
      QueryResult(findDuplicateProjects(path, conn, keyspace), findSimilarProjects(path))
    } else {
      val fileQuery = new FileQuery(conn, bblfshClient, feClient, docFreqPath, log, keyspace, tables, mode)
      fileQuery.find(path, fnFilter)
    }
  }

  /**
    * Finds duplicate files among hashed repositories
    * It is used one query per distinct file
    *
    * @param conn Database connections
    * @param advancedCql use advanced cql or not (supported only by Apache Cassandra)
    * @param ccDirPath directory for connected components
    * @return
    */
  def report(conn: Session, advancedCql: Boolean, ccDirPath: String): ReportResult = {
    val report = new Report(conn, log, keyspace, tables)

    log.info(s"Report duplicate items from DB $keyspace")
    val duplicates = advancedCql match {
      case false => ReportDuplicates(report.findAllDuplicateItems())
      case true => ReportDuplicates(report.reportCassandraGroupBy())
    }
    log.info(s"${duplicates.size} duplicate SHA1s")

    val similarities =
      report.findSimilarItems(ccDirPath, Gemini.fileSimilarityMode) ++
      report.findSimilarItems(ccDirPath, Gemini.funcSimilarityMode)

    ReportResult(duplicates, similarities)
  }

  def isDBEmpty(session: Session, mode: String): Boolean = {
    var row = session.execute(s"select count(*) from $keyspace.${tables.docFreq} where id='$mode' limit 1").one()
    if (row.getLong(0) > 0) {
      return false
    }

    row = session.execute(s"select count(*) from $keyspace.${tables.hashtables}_$mode").one()
    if (row.getLong(0) > 0) {
      return false
    }

    true
  }

  def cleanDB(session: Session, mode: String): Unit = {
    session.execute(s"delete from $keyspace.${tables.docFreq} where id='$mode'")
    session.execute(s"truncate table $keyspace.${tables.hashtables}_$mode")
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

  val fileSimilarityMode: String = "file"
  val funcSimilarityMode: String = "func"
  val similarityModes = Seq(Gemini.fileSimilarityMode,  Gemini.funcSimilarityMode)


  val tables = Tables(
    "meta",
    "hashtables",
    "features_docs",
    "features_freq",
    MetaCols("sha1", "repo", "commit", "path"),
    HashtablesCols("sha1", "hashtable", "value"),
    FeaturesDocsCols("id", "docs"),
    FeaturesFreqCols("id", "feature", "weight")
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

  def findDuplicateProjects(in: File, conn: Session, keyspace: String): Iterable[RepoFile] = {
    //TODO(bzz): project is duplicate if it has all it's files in some other projects
    throw new UnsupportedOperationException("Finding duplicate repositories is no implemented yet.")
  }

  def findSimilarProjects(in: File): Iterable[SimilarItem] = {
    throw new UnsupportedOperationException("Finding similar repositories is no implemented yet.")
  }

  // item for functions is sha1_func_name:line
  def splitFuncItem(item: String): (String, String, String) = {
    val Array(sha1, rest) = item.split("_", 2)
    val Array(name, line) = rest.split(":")

    (sha1, name, line)
  }
}

