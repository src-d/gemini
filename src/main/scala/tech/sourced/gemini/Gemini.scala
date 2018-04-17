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
import tech.sourced.featurext.generated.service.FeatureExtractorGrpc.FeatureExtractor
import tech.sourced.featurext.generated.service._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.{Duration, SECONDS}
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.parsing.json._

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
  def query(inPath: String,
            conn: Session,
            bblfshClient: BblfshClient,
            feClient: FeatureExtractor,
            docFreqPath: String = "",
            paramsFilePath: String = "", // TODO remove when we implement hash
            htNum: Int = 0,
            bandSize: Int = 0): (Iterable[RepoFile], Iterable[RepoFile]) = {
    val path = new File(inPath)
    if (path.isDirectory) {
      //TODO: implement based on Apollo
      (findDuplicateProjects(path, conn, keyspace), findSimilarProjects(path))
    } else {
      val duplicates = findDuplicateItemForFile(path, conn, keyspace)
      var similarShas = Array[String]()

      if (docFreqPath != "" && paramsFilePath != "") {
        similarShas =
          findSimilarForFile(path, conn, bblfshClient, feClient, docFreqPath, paramsFilePath, htNum, bandSize)
          .getOrElse(Array[String]())
      } else {
        log.warn("Document frequency or parameters for weighted min hash wasn't provided. Skip similarity query")
      }

      // value for sha1 in hashtables is path@sha1, but we need only sha1
      similarShas = similarShas.map(_.split("@")(1))
      // remove duplicates from similar
      if (similarShas.isEmpty) {
        log.info("No similar sha1s")
      } else {
        val duplicatedShas = duplicates.map(_.sha).toArray
        similarShas = similarShas.filterNot(sha => duplicatedShas.contains(sha))
      }

      val similar = similarShas.flatMap(sha1 => findDuplicateItemForBlobHash(sha1, conn, keyspace))

      (duplicates, similar)
    }
  }

  def findSimilarForFile(
                          file: File,
                          conn: Session,
                          bblfshClient: BblfshClient,
                          feClient: FeatureExtractor,
                          docFreqFile: String,
                          paramsFile: String,
                          htnum: Int,
                          bandSize: Int): Option[Array[String]] = {

    val features = extractUAST(file, bblfshClient).map { uast =>
      log.debug(s"uast received: ${uast.toString}")

      val result = extractFeatures(feClient, uast)
      log.debug(s"features: ${result.toString}")

      result
    }

    features.map { featuresList =>
      findSimilarFiles(featuresList, conn, docFreqFile, paramsFile, htnum, bandSize)
    }
  }

  private def findSimilarFiles(
                                featuresList: Iterable[Feature],
                                conn: Session,
                                docFreqFile: String,
                                paramsFile: String,
                                htnum: Int,
                                bandSize: Int
                              ): Array[String] = {

    if (featuresList.isEmpty) {
      log.warn("file doesn't contain features")
      Array[String]()
    } else {
      val wmh = hashFile(featuresList, new File(docFreqFile), new File(paramsFile))

      // split hash to bands
      // a little bit verbose because java doesn't have uint32 type
      // in apollo it's one-liner:
      // https://github.com/src-d/apollo/blob/57e52394783d73e38cf1f862afc0166724991fd5/apollo/query.py#L35
      val bands = (0 until htnum).map { i =>
        val from = i * bandSize
        val to = (i + 1) * bandSize
        val band = (from until to).foldLeft(Array.empty[Byte]) { (arr, j) =>
          arr ++ MathUtil.toUInt32ByteArray(wmh(j)(0)) ++ MathUtil.toUInt32ByteArray(wmh(j)(1))
        }
        band
      }

      log.info("Looking for similar items")
      val similar = bands.zipWithIndex.foldLeft(Set.empty[String]) { case (similar, (band, i)) =>
        val table = "hashtables"
        val cql = s"SELECT sha1 FROM $keyspace.$table WHERE hashtable=$i AND value=0x${MathUtil.bytes2hex(band)}"
        log.debug(cql)

        val sha1s = conn.execute(new SimpleStatement(cql))
          .asScala
          .map(row => row.getString("sha1"))

        similar ++ sha1s
      }
      log.info(s"Fetched ${similar.size} items")

      similar.toArray
    }
  }

  def extractUAST(file: File, client: BblfshClient): Option[Node] = {
    val byteArray = Files.readAllBytes(file.toPath)
    log.info(s"Extracting UAST")
    try {
      val resp = client.parse(file.getName, new String(byteArray))
      if (resp.errors.nonEmpty) {
        val errors = resp.errors.mkString(",")
        log.error(s"bblfsh errors: ${errors}")
      }

      resp.uast
    } catch {
      case NonFatal(e) => {
        log.error(s"bblfsh error: ${e.toString}")
        None
      }
    }
  }

  def extractFeatures(client: FeatureExtractor, uast: Node): Iterable[Feature] = {
    val idRequest = IdentifiersRequest(uast=Some(uast), docfreqThreshold=5)
    val litRequest = LiteralsRequest(uast=Some(uast), docfreqThreshold=5)
    val graphletRequest = GraphletRequest(uast=Some(uast), docfreqThreshold=5)
    client.identifiers(idRequest)

    try {
      val features = for {
        idResponse <- client.identifiers(idRequest)
        litResponse <- client.literals(litRequest)
        graphletResponse <- client.graphlet(graphletRequest)
      } yield idResponse.features ++ litResponse.features ++ graphletResponse.features

      Await.result(features, Duration(30, SECONDS))
    } catch {
      case NonFatal(e) => {
        log.error(s"feature extractor error: ${e.toString}")
        Iterable[Feature]()
      }
    }
  }

  def mustParseJSON[T: ClassTag](input: String): T = {
    JSON.parseFull(input) match {
      case Some(res: T) => res
      case Some(_) => throw new Exception("incorrect json")
      case None => throw new Exception("can't parse json")
    }
  }

  def parseDocFreq(file: File): (Int, Map[String, Double], List[String]) = {
    log.info("Reading docFreq")
    val docFreqByteArray = Files.readAllBytes(file.toPath)
    val docFreqMap = mustParseJSON[Map[_, _]](new String(docFreqByteArray))
      .asInstanceOf[Map[String, Any]]

    val docs = docFreqMap.get("docs") match {
      case Some(v) => v.asInstanceOf[Double].toInt
      case None => throw new Exception("can not parse docs in docFreq")
    }
    val df = docFreqMap.get("df") match {
      case Some(v) => v.asInstanceOf[Map[String, Double]]
      case None => throw new Exception("can not parse df in docFreq")
    }
    val tokens = docFreqMap.get("tokens") match {
      case Some(v) => v.asInstanceOf[List[String]]
      case None => throw new Exception("can not parse tokens in docFreq")
    }

    (docs, df, tokens)
  }

  def parseParams(file: File): Map[String, List[List[Double]]] = {
    log.info("Reading params")
    val paramsByteArray = Files.readAllBytes(file.toPath)
    mustParseJSON[Map[_, _]](new String(paramsByteArray))
      .asInstanceOf[Map[String, List[List[Double]]]]
  }

  def hashFile(features: Iterable[Feature], docFreq: File, paramsFile: File): Array[Array[Long]] = {
    val (docs, df, tokens) = parseDocFreq(docFreq)

    val bag = mutable.ArrayBuffer.fill(tokens.size)(0.toDouble)
    features.foreach { feature =>
      val idx = tokens.indexOf(feature.name)
      log.debug(s"name: ${feature.name}, weight: ${feature.weight}")
      idx match {
        case idx if idx >= 0 => {
          val tf = feature.weight.toDouble

          bag(idx) = MathUtil.logTFlogIDF(tf, df(feature.name), docs)
        }
        case _ =>
      }
    }

    log.info(s"Bag size: ${bag.size}")
    log.info("Started hashing file")
    log.debug(s"Bag: ${bag.mkString(",")}")

    // TODO don't use params file when we implement our own hash
    val params = parseParams(paramsFile)
    val wmh = new WeightedMinHash(
      bag.size,
      params("rs").length,
      params("rs") map (_.toArray) toArray,
      params("ln_cs") map (_.toArray) toArray,
      params("betas") map (_.toArray) toArray)

    val hash = wmh.hash(bag.toArray)
    log.info("Finished hashing file")
    hash
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

  /**
    * Return connected components from DB hashtables
    *
    * @param conn Database connections
    * @return Tuple with:
    *         - Map of connected components groupId to list of elements
    *         - Map of element ids to list of bucket indices
    */
  def findConnectedComponents(conn: Session): (Map[Int, Set[Int]], Map[Int, List[Int]]) = {
    val cc = new DBConnectedComponents(log, conn, "hashtables", keyspace)
    val buckets = cc.makeBuckets()
    val elsToBuckets = cc.elementsToBuckets(buckets)

    (cc.findInBuckets(buckets, elsToBuckets), elsToBuckets)
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
  val defaultTable: String = "meta"
  val defaultBblfshHost: String = "127.0.0.1"
  val defaultBblfshPort: Int = 9432
  val defaultFeHost: String = "127.0.0.1"
  val defaultFePort: Int = 9001

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

  def findSimilarProjects(in: File): Iterable[RepoFile] = {
    throw new UnsupportedOperationException("Finding similar repositories is no implemented yet.")
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

