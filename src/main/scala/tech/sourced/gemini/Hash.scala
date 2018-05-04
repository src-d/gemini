package tech.sourced.gemini

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.slf4j.{Logger => Slf4jLogger}
import tech.sourced.engine._
import tech.sourced.featurext.SparkFEClient
import tech.sourced.featurext.generated.service.Feature

case class RDDFeatureKey(token: String, doc: String)
case class RDDFeature(key: RDDFeatureKey, weight: Long)
case class RDDHash(doc: String, wmh: Array[Array[Long]])
case class HashResult(files: DataFrame, hashes: RDD[RDDHash], docFreq: OrderedDocFreq)

/**
  * Implements hashing repositories using Apache Spark
  *
  * @param session spark session
  * @param log
  */
class Hash(session: SparkSession, log: Slf4jLogger) {
  import session.implicits._

  /**
    * Calculates hashes and docFreq
    *
    * @param repos DataFrame with engine.getRepositories schema
    */
  def forRepos(repos: DataFrame): HashResult = {
    val files = filesForRepos(repos).cache()
    val uasts = extractUast(files).cache()
    val features = extractFeatures(uasts).cache()
    val docFreq = makeDocFreq(uasts, features)
    val hashes = hashFeatures(docFreq, features)

    HashResult(files, hashes, docFreq)
  }

  /**
    * Save hashing results to database
    *
    * @param hashResult
    * @param keyspace
    * @param tables
    */
  def save(hashResult: HashResult, keyspace: String, tables: Tables): Unit ={
    saveMeta(hashResult.files, keyspace, tables)
    saveDocFreq(hashResult.docFreq)
    saveHashes(hashResult.hashes, keyspace, tables)
  }

  protected def filesForRepos(repos: DataFrame): DataFrame =
    repos
      .getHEAD
      .getCommits
      .getTreeEntries
      .getBlobs
      .filter('is_binary === false)

  protected def extractUast(files: DataFrame): DataFrame = {
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

  // TODO(max): Try to use DF here instead
  protected def extractFeatures(uastsDF: DataFrame): RDD[RDDFeature] = {
    var feConfig = SparkFEClient.getConfig(session)
    val rows = uastsDF.flatMap { row =>
      val uastArr = row.getAs[Seq[Array[Byte]]]("uast")
      val features = uastArr.flatMap { byteArr =>
        val uast = Node.parseFrom(byteArr)
        SparkFEClient.extract(uast, feConfig)
      }
      features.map(f => RDDFeature(RDDFeatureKey(f.name, row.getAs[String]("document")), f.weight.toLong))
    }

    rows.rdd
  }

  // TODO(max): Try to use DF here instead
  protected def makeDocFreq(uasts: DataFrame, features: RDD[RDDFeature]): OrderedDocFreq = {
    log.info("creating document frequencies")

    val docs = uasts.select("document").distinct().count()

    val df = features
      .map(row => (row.key.token, row.key.doc))
      .distinct()
      .map(row => (row._1, 1))
      .reduceByKey((a, b) => a + b)
      .collectAsMap()

    new OrderedDocFreq(docs.toInt, df.keys.toArray[String], df.toMap[String, Int])
  }

  // TODO(max): Try to use DF here instead
  protected def hashFeatures(
                              docFreq: OrderedDocFreq,
                              featuresRdd: RDD[RDDFeature]): RDD[RDDHash] = {

    val tf = featuresRdd
      .map(row => (row.key, row.weight))
      .reduceByKey((a, b) => a + b)

    val tfidf = tf
      .map(row => (row._1.doc, Feature(row._1.token, row._2)))
      .groupByKey()
      .map { row =>
        val doc = row._1
        val features = row._2

        RDDHash(doc, FeaturesHash.hashFeatures(docFreq, features))
      }

    tfidf
  }

  protected def saveDocFreq(docFreq: OrderedDocFreq): Unit = {
    log.info("save document frequencies")
    // TODO(max) replace with DB later
    docFreq.saveToJson(Gemini.defaultDocFreqFile)
  }

  protected def saveMeta(files: DataFrame, keyspace: String, tables: Tables): Unit = {
    log.info("save meta")

    val cols = tables.metaCols
    val renamedFiles = files
      .select("blob_id", "repository_id", "commit_hash", "path")
      .withColumnRenamed("blob_id", cols.sha)
      .withColumnRenamed("repository_id", cols.repo)
      .withColumnRenamed("commit_hash", cols.commit)
      .withColumnRenamed("path", cols.path)

    val approxFileCount = renamedFiles.rdd.countApprox(10000L)
    log.info(s"Writing $approxFileCount files to DB")
    renamedFiles.write
      .mode("append")
      .cassandraFormat(tables.meta, keyspace)
      .save()
  }

  protected def saveHashes(rdd: RDD[RDDHash], keyspace: String, tables: Tables): Unit = {
    log.info("save hashtables")

    val cols = tables.hashtablesCols
    rdd.flatMap(row => {
      val RDDHash(doc, wmh) = row
      FeaturesHash.wmhToBands(wmh).zipWithIndex.map{ case(band, i) => (doc, i, band) }
    })
      .toDF(cols.sha, cols.hashtable, cols.value)
      .write
      .mode("append")
      .cassandraFormat(tables.hashtables, keyspace)
      .save()
  }
}