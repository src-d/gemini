package tech.sourced.gemini


import com.datastax.spark.connector.cql.CassandraConnector
import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger => Slf4jLogger}
import tech.sourced.engine._
import tech.sourced.featurext.SparkFEClient
import tech.sourced.featurext.generated.service.Feature
import tech.sourced.gemini.util.MapAccumulator
import scala.collection.JavaConverters._


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
class Hash(session: SparkSession, log: Slf4jLogger, docFreqPath: String = "") {
  import session.implicits._

  def report(header: String, countProcessed: Long, skipped: MapAccumulator): Unit = {
    log.warn(header)
    val skippedSeq = skipped.value.toSeq
    val countSkipped = skippedSeq.map(_._2).sum
    log.warn(s"Processed: $countProcessed, skipped: $countSkipped")
    skippedSeq.sortBy(-_._2) foreach { case (key, value) => log.warn(s"\t$key -> $value") }
  }

  /**
    * Calculates hashes and docFreq
    *
    * @param repos DataFrame with engine.getRepositories schema
    */
  def forRepos(repos: DataFrame): HashResult = {
    val feSkippedFiles = mapAccumulator(session.sparkContext, "FE skipped files")

    val files = filesForRepos(repos).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val uasts = extractUast(files).persist(StorageLevel.MEMORY_AND_DISK_SER)
    val features = extractFeatures(uasts, feSkippedFiles).cache()
    report("Feature Extraction exceptions", features.count, feSkippedFiles)

    val docFreq = makeDocFreq(uasts, features)
    val hashes = hashFeatures(docFreq, features)

    HashResult(files, hashes, docFreq.value)
  }

  /**
    * Save hashing results to database
    *
    * @param hashResult
    * @param keyspace
    * @param tables
    */
  def save(hashResult: HashResult, keyspace: String, tables: Tables, docFreqPath: String): Unit ={
    saveMeta(hashResult.files, keyspace, tables)
    if (docFreqPath.isEmpty) {
      saveDocFreqToDB(hashResult.docFreq, keyspace, tables)
    } else {
      log.warn(s"save document frequencies to JSON")
      hashResult.docFreq.saveToJson(docFreqPath)
    }
    saveHashes(hashResult.hashes, keyspace, tables)
  }

  protected def filesForRepos(repos: DataFrame): DataFrame = {
    log.warn("Listing files")

    repos
      .getHEAD
      .getCommits
      .getTreeEntries
      .getBlobs
      .filter('is_binary === false)
  }

  protected def extractUast(files: DataFrame): DataFrame = {
    log.warn("Extracting UASTs")

    // TODO(max): get languages from bblfsh directly as soon as
    // https://github.com/bblfsh/client-scala/issues/68 resolved
    val langs = Seq("Java", "Python", "Go", "JavaScript", "TypeScript", "Ruby", "Bash", "Php")

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
  protected def extractFeatures(uastsDF: DataFrame, skippedFiles: MapAccumulator): RDD[RDDFeature] = {
    log.warn("Extracting features")

    val feConfig = SparkFEClient.getConfig(session)
    val rows = uastsDF.flatMap { row =>
      val uastArr = row.getAs[Seq[Array[Byte]]]("uast")
      val features = uastArr.flatMap { byteArr =>
        val uast = Node.parseFrom(byteArr)
        SparkFEClient.extract(uast, feConfig, Some(skippedFiles))
      }
      features.map(f => RDDFeature(RDDFeatureKey(f.name, row.getAs[String]("document")), f.weight.toLong))
    }

    rows.rdd
  }

  // TODO(max): Try to use DF here instead
  protected def makeDocFreq(uasts: DataFrame, features: RDD[RDDFeature]): Broadcast[OrderedDocFreq] = {
    log.warn("creating document frequencies")
    val docs = uasts.select("document").distinct().count()
    val df = features
      .map(row => (row.key.token, row.key.doc))
      .distinct()
      .map(row => (row._1, 1))
      .reduceByKey((a, b) => a + b)
      .collectAsMap()
    val tokens = df.keys.toArray.sorted
    session.sparkContext.broadcast(OrderedDocFreq(docs.toInt, tokens, df))
  }

  // TODO(max): Try to use DF here instead
  protected def hashFeatures(
                              docFreq: Broadcast[OrderedDocFreq],
                              featuresRdd: RDD[RDDFeature]): RDD[RDDHash] = {
    log.warn("hashing features")
    val tf = featuresRdd
      .map(row => (row.key, row.weight))
      .reduceByKey(_ + _)

    val tfIdf = tf
      .map(row => (row._1.doc, Feature(row._1.token, row._2)))
      .groupByKey(session.sparkContext.defaultParallelism)
      .mapPartitions { partIter =>
        val wmh = FeaturesHash.initWmh(docFreq.value.tokens.size) // ~1.6 Gb (for 1 PGA bucket)
        partIter.map { case (doc, features) =>
          RDDHash(doc, wmh.hash(FeaturesHash.toBagOfFeatures(features, docFreq.value)))
        }
    }
    tfIdf
  }

  protected def saveDocFreqToDB(docFreq: OrderedDocFreq, keyspace: String, tables: Tables): Unit = {
    log.warn(s"save document frequencies to DB")
    CassandraConnector(session.sparkContext).withSessionDo { cassandra =>
      val cols = tables.docFreqCols
      val javaMap = docFreq.df.asJava

      cassandra.execute(
        s"INSERT INTO $keyspace.${tables.docFreq} (${cols.id}, ${cols.docs}, ${cols.df}) VALUES (?, ?, ?)",
        Gemini.docFreqId, int2Integer(docFreq.docs), javaMap
      )
    }
  }

  protected def saveMeta(files: DataFrame, keyspace: String, tables: Tables): Unit = {
    log.warn("save meta to DB")

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
    log.warn("save hashtables to DB")

    val cols = tables.hashtablesCols
    rdd
      .flatMap { case RDDHash(doc, wmh) =>
        FeaturesHash.wmhToBands(wmh).zipWithIndex.map{ case (band, i) => (doc, i, band) }
      }
      .toDF(cols.sha, cols.hashtable, cols.value)
      .write
      .mode("append")
      .cassandraFormat(tables.hashtables, keyspace)
      .save()
  }

   def mapAccumulator(sc: SparkContext, name: String): MapAccumulator = {
    val acc = new MapAccumulator
    sc.register(acc, name)
    acc
  }
}

object Hash {
    def apply(s: SparkSession, log: Slf4jLogger): Hash = new Hash(s, log)
}
