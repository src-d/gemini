package tech.sourced.gemini

import java.io.{File, FileInputStream}
import java.nio.file.Files

import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.{Session, SimpleStatement}
import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.bblfsh.client.BblfshClient
import org.eclipse.jgit.lib.Constants.OBJ_BLOB
import org.eclipse.jgit.lib.ObjectInserter
import org.slf4j.{Logger => Slf4jLogger}
import tech.sourced.engine._
import tech.sourced.featurext.generated.service.FeatureExtractorGrpc.FeatureExtractor
import tech.sourced.featurext.generated.service._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source
import scala.util.control.NonFatal
import tech.sourced.featurext.{FEClient, SparkFEClient}

case class SparkFeature(key: Array[String], weight: Long)

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
      .filter('is_binary === false)

  def sparkExtractUast(files: DataFrame): DataFrame = {
    // TODO(max): get languages from bblfsh directly as soon as
    // https://github.com/bblfsh/client-scala/issues/68 resolved
    val langs = Array("Java", "Python", "Go", "JavaScript", "TypeScript", "Ruby", "Bash", "Php")

    files
      .dropDuplicates("blob_id")
      .classifyLanguages
      .filter('lang.isin(langs: _*))
      .extractUASTs
      .select("repository_id", "path", "blob_id", "uast")
      .filter(_.getAs[Seq[Array[Byte]]]("uast").nonEmpty)
      .withColumn("document", expr("CONCAT(repository_id, '//', path, '@', blob_id)"))
      .select("document", "uast")
  }

  // Try to use DF here instead
  def sparkFeatures(uastsDf: DataFrame): RDD[SparkFeature] = {
    var feConfig = SparkFEClient.getConfig(session)
    val rows = uastsDf.flatMap { row =>
      val uastArr = row.getSeq[Array[Byte]](1)
      val features = uastArr.flatMap { byteArr =>
        val uast = Node.parseFrom(byteArr)
        SparkFEClient.extract(uast, feConfig)
      }
      features.map(f => SparkFeature(Array[String](f.name, row(0).asInstanceOf[String]), f.weight.toLong))
    }

    rows.rdd
  }

  def makeDocFreq(uasts: DataFrame, features: RDD[SparkFeature]): OrderedDocFreq = {
    val docs = uasts.select("document").distinct().count()

    val df = features
      .map(row => (row.key(0), row.key(1)))
      .distinct()
      .map(row => (row._1, 1))
      .reduceByKey((a, b) => a + b)
      .collectAsMap()

    new OrderedDocFreq(docs.toInt, df.keys.toArray, df.toMap)
  }

  def saveDocFreq(docFreq: OrderedDocFreq): Unit = {
    // TODO replace with DB later
    docFreq.saveToJson(Gemini.defaultDocFreqFile)
  }

  def save(files: DataFrame): Unit = {
    val renamedFiles = files
      .select("blob_id", "repository_id", "commit_hash", "path")
      .withColumnRenamed("blob_id", meta.sha)
      .withColumnRenamed("repository_id", meta.repo)
      .withColumnRenamed("commit_hash", meta.commit)
      .withColumnRenamed("path", meta.path)

    val approxFileCount = renamedFiles.rdd.countApprox(10000L)
    log.info(s"Writing $approxFileCount files to DB")
    renamedFiles.write
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
            paramsPath: String = "", // TODO remove when we implement hash
            htNum: Int = 0,
            bandSize: Int = 0): (Iterable[RepoFile], Iterable[RepoFile]) = {
    val path = new File(inPath)
    if (path.isDirectory) {
      (findDuplicateProjects(path, conn, keyspace), findSimilarProjects(path))
    } else {
      val duplicates = findDuplicatesOfFile(path, conn, keyspace)
      val duplicatedShas = duplicates.map(_.sha).toSeq

      val similarShas = findSimilarForFile(path, conn, bblfshClient, feClient, docFreqPath, paramsPath, htNum, bandSize)
        .filterNot(sha => duplicatedShas.contains(sha))
        .map(_.split("@")(1)) // value for sha1 in Apollo hashtables is 'path@sha1', but we need only hashes

      log.info(s"${similarShas.length} SHA1's found to be similar")

      val similar = similarShas.flatMap(sha1 => findDuplicatesOfBlobHash(sha1, conn, keyspace))
      (duplicates, similar)
    }
  }

  def findSimilarForFile(
                          file: File,
                          conn: Session,
                          bblfshClient: BblfshClient,
                          feClient: FeatureExtractor,
                          docFreqPath: String,
                          paramsPath: String,
                          htnum: Int,
                          bandSize: Int): Seq[String] = {
    val docFreqFile = new File(docFreqPath)
    val paramsFile = new File(paramsPath)
    if (!docFreqFile.exists() || !paramsFile.exists()) {
      log.warn("Document frequency or parameters for weighted min hash wasn't provided. Skip similarity query")
      Seq()
    } else {
      extractUAST(file, bblfshClient) match {
        case Some(node) =>
          val featuresList = extractFeatures(feClient, node)
          findSimilarFiles(featuresList, conn, docFreqFile, paramsFile, htnum, bandSize)
        case _ => Seq()
      }
    }
  }

  private def findSimilarFiles(
                                featuresList: Iterable[Feature],
                                conn: Session,
                                docFreqFile: File,
                                paramsFile: File,
                                htnum: Int,
                                bandSize: Int
                              ): Seq[String] = {

    if (featuresList.isEmpty) {
      log.warn("file doesn't contain features")
      Seq()
    } else {
      val wmh = hashFile(featuresList, docFreqFile, paramsFile)

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
      val similar = bands.zipWithIndex.foldLeft(Set[String]()) { case (sim, (band, i)) =>
        val table = "hashtables"
        val cql = s"SELECT sha1 FROM $keyspace.$table WHERE hashtable=$i AND value=0x${MathUtil.bytes2hex(band)}"
        log.debug(cql)

        val sha1s = conn.execute(new SimpleStatement(cql))
          .asScala
          .map(_.getString("sha1"))

        sim ++ sha1s
      }
      log.info(s"Fetched ${similar.size} items")

      similar.toSeq
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
    log.debug(s"uast received: ${uast.toString}")
    val result = FEClient.extract(uast, client, log)
    log.debug(s"features: ${result.toString}")
    result
  }

  def parseParams(file: File): Map[String, List[List[Double]]] = {
    log.info("Reading params")
    JSONUtils.parseFile[Map[_, _]](file)
      .asInstanceOf[Map[String, List[List[Double]]]]
  }


  def hashFile(features: Iterable[Feature], docFreq: File, paramsFile: File): Array[Array[Long]] = {
    log.info("Reading docFreq")
    val OrderedDocFreq(docs, tokens, df) = OrderedDocFreq.fromJson(docFreq)

    val bag = mutable.ArrayBuffer.fill(tokens.size)(0.toDouble)
    features.foreach { feature =>
      val index = tokens.indexOf(feature.name)
      log.debug(s"name: ${feature.name}, weight: ${feature.weight}")
      index match {
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
      params("rs").map(_.toArray).toArray,
      params("ln_cs").map(_.toArray).toArray,
      params("betas").map(_.toArray).toArray)

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
        findDuplicatesOfBlobHash(item.sha, conn, keyspace)
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
    val cc = new DBConnectedComponents(log, conn, "hashtables", keyspace)
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
  val defaultDocFreqFile: String = "docfreq.json"
  val defaultParamsFile: String = "params.json"

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
        val dupes = findDuplicatesOfBlobHash(r.getString(hash), conn, keyspace)
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

  def findDuplicatesOfFile(file: File, conn: Session, keyspace: String): Iterable[RepoFile] = {
    findDuplicatesOfBlobHash(computeSha1(file), conn, keyspace)
  }

  def findDuplicatesOfBlobHash(sha: String, conn: Session, keyspace: String): Iterable[RepoFile] = {
    val query = QueryBuilder.select().all().from(keyspace, defaultTable)
      .where(QueryBuilder.eq(meta.sha, sha))

    conn.execute(query).asScala.map { row =>
      RepoFile(row.getString(meta.repo), row.getString(meta.commit), row.getString(meta.path), row.getString(meta.sha))
    }
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
      val elems = sha1s.map(st => s"'$st'").mkString(",")
      val query = s"select sha1, repo, commit, path from $keyspace.$defaultTable where sha1 in ($elems)"

      conn
        .execute(new SimpleStatement(query))
        .asScala
        .map { row =>
          RepoFile(row.getString(meta.repo), row.getString(meta.commit),
            row.getString(meta.path), row.getString(meta.sha))
        }
    })
  }
}

