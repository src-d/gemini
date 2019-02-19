package tech.sourced.gemini


import com.datastax.spark.connector.cql.CassandraConnector
import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{udf => sparkUdf} // udf name conflicts with engine
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.bblfsh.client.BblfshClient
import org.slf4j.{Logger => Slf4jLogger}
import tech.sourced.engine._
import tech.sourced.enry.Enry
import tech.sourced.featurext.{Extractor, FEClient, SparkFEClient}
import tech.sourced.featurext.generated.service.Feature
import tech.sourced.gemini.util.MapAccumulator

import scala.collection.mutable


case class RDDFeatureKey(token: String, doc: String)

case class RDDFeature(key: RDDFeatureKey, weight: Long)

case class RDDHash(doc: String, wmh: Array[Array[Long]])

case class HashResult(files: DataFrame, hashes: Dataset[RDDHash], docFreq: OrderedDocFreq)

/**
  * Implements hashing repositories using Apache Spark
  *
  * @param session     spark session
  * @param log         logger implementation
  * @param mode        Gemini.fileSimilarityMode or Gemini.funcSimilarityMode
  * @param docFreqPath path to DocFreq
  */
class Hash(session: SparkSession,
           log: Slf4jLogger,
           mode: String = Gemini.fileSimilarityMode,
           docFreqPath: String = "") {

  // very small files produce too much false positives
  val fileSizeThresholdBytes = 500

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
    var uasts = extractUast(files).persist(StorageLevel.MEMORY_AND_DISK_SER)

    var extractors = FEClient.fileLevelExtractors
    if (mode == Gemini.funcSimilarityMode) {
      log.warn(s"Mode: $mode")
      uasts = uasts
        .flatMap { row =>
          val doc = row(0).asInstanceOf[String]
          val uastArr = row(1).asInstanceOf[Seq[Array[Byte]]]

          uastArr.map(Node.parseFrom).flatMap { uast =>
            Hash.extractFunctions(uast).map { case (fnName, fnUast) =>
              val docFn = s"${doc}_$fnName:${fnUast.getStartPosition.line}"
              (docFn, Seq(fnUast.toByteArray))
            }
          }
        }
        .toDF("document", "uast")
      extractors = FEClient.funcLevelExtractors
    }
    val features = extractFeatures(uasts, extractors, feSkippedFiles).cache()
    report("Feature Extraction exceptions", features.count, feSkippedFiles)

    val docFreq = makeDocFreq(uasts, features)

    val FeaturesHashOpts(sampleSize, _, _) = mode match {
      case Gemini.fileSimilarityMode => FeaturesHash.fileParams
      case Gemini.funcSimilarityMode => FeaturesHash.funcParams
    }
    val hashes = hashFeaturesDF(docFreq.value, features, sampleSize)

    HashResult(files, hashes, docFreq.value)
  }

  /**
    * Save hashing results to database
    *
    * @param hashResult
    * @param keyspace
    * @param tables
    * @param docFreqPath
    */
  def save(hashResult: HashResult,
           keyspace: String,
           tables: Tables,
           docFreqPath: String): Unit = {
    saveMeta(hashResult.files, keyspace, tables)
    if (docFreqPath.isEmpty) {
      saveDocFreqToDB(hashResult.docFreq, keyspace, tables)
    } else {
      log.warn(s"save document frequencies in JSON to ${docFreqPath}")
      hashResult.docFreq.saveToJson(docFreqPath)
    }
    saveHashes(hashResult.hashes.rdd, keyspace, tables)
  }

  protected def filesForRepos(repos: DataFrame): DataFrame = {
    log.warn("Listing files")

    val fileSizeUdf = sparkUdf { (content: Array[Byte]) => content.size }

    repos
      .getHEAD
      .getCommits
      .getTreeEntries
      .getBlobs
      .filter('is_binary === false)
      .filter(r => !Enry.isVendor(r.getAs[String]("path")))
      .withColumn("content_size", fileSizeUdf('content))
      .filter('content_size !== 0) // empty files only pollute results
  }

  protected def extractUast(files: DataFrame): DataFrame = {
    log.warn("Extracting UASTs")

    val blobs = files.dropDuplicates("blob_id")

    val filteredBlobs = if (mode == Gemini.fileSimilarityMode) {
      blobs.filter('content_size > fileSizeThresholdBytes)
    } else {
      blobs
    }

    filteredBlobs
      .classifyLanguages
      .filter('lang.isNotNull)
      .extractUASTs
      .select("repository_id", "path", "blob_id", "uast")
      .filter(_.getAs[Seq[Array[Byte]]]("uast").nonEmpty)
      .withColumn("document", expr("CONCAT(repository_id, '//', path, '@', blob_id)"))
      .select("document", "uast")
  }

  def extractFeatures(uastsDF: DataFrame, extractors: Seq[Extractor], skippedFiles: MapAccumulator): DataFrame = {
    log.warn("Extracting features")

    val feConfig = SparkFEClient.getConfig(session)
    uastsDF
      .flatMap { row =>
        val uastArr = row.getAs[Seq[Array[Byte]]]("uast")
        val features = uastArr.flatMap { byteArr =>
          val uast = Node.parseFrom(byteArr)
          SparkFEClient.extract(uast, feConfig, extractors, Some(skippedFiles))
        }
        features
          // 65535 is the limit for a key in Scylla
          // key = (hash mode, feature.name)
          // and it's just sane to remove such huge name
          .filter(_.name.length < 65530)
          .map(feat => (feat.name, row.getAs[String]("document"), feat.weight.toLong))
      }
      .toDF("feature", "doc", "weight")
  }

  def makeDocFreq(uasts: DataFrame, features: DataFrame): Broadcast[OrderedDocFreq] = {
    log.warn("creating document frequencies")
    val docs = uasts.select("document").distinct().count()
    val df = features
      .select("feature", "doc")
      .distinct
      .groupBy("feature")
      .agg(count("*").alias("cnt"))
      .map(row => (row.getAs[String]("feature"), row.getAs[Long]("cnt").toInt))
      .collect.toMap
    val tokens = df.keys.toArray.sorted
    session.sparkContext.broadcast(OrderedDocFreq(docs.toInt, tokens, df))
  }

  /**
    * Hash features using RDD representation.
    *
    * This impl is retrofitted to new Dataset API so it is a dorp-in replacement for hashFeaturesDF(),
    * but it uses RDD inside in order to benefit from groupByKey/mapPartitions perf optimizations.
    *
    * This is expected to scale better to full PGA, as hashing is done in parallel for each partition.
    *
    * @param docFreq
    * @param features
    * @return
    */
  def hashFeaturesRDD(
    docFreq: OrderedDocFreq,
    features: DataFrame,
    sampleSize: Int
  ): Dataset[RDDHash] = {
    log.warn("hashing features")

    val wmh = makeBroadcastedWmh(docFreq.tokens.size, sampleSize)

    val tf = features.rdd
      .map { case Row(feature: String, doc: String, weight: Long) => (RDDFeatureKey(feature, doc), weight) }
      .reduceByKey(_ + _)

    val tfIdf = tf
      .map(row => (row._1.doc, Feature(row._1.token, row._2)))
      .groupByKey(session.sparkContext.defaultParallelism)
      .mapPartitions { partIter =>
        partIter.map { case (doc, features) =>
          RDDHash(doc, wmh.value.hash(FeaturesHash.toBagOfFeatures(features.iterator, docFreq)))
        }
      }
    tfIdf.toDS()
  }

  /**
    * Hash features using Dataset representation.
    */
  def hashFeaturesDF(
    docFreq: OrderedDocFreq,
    features: DataFrame,
    sampleSize: Int
  ): Dataset[RDDHash] = {
    log.warn("hashing features")

    val wmh = makeBroadcastedWmh(docFreq.tokens.size, sampleSize)
    val tf = features.groupBy("feature", "doc").sum("weight").alias("weight")
    val tfIdf = tf
      .map { case Row(token: String, doc: String, weight: Long) => (doc, Feature(token, weight)) }
      .groupByKey { case (doc, _) => doc }
      .mapGroups { (doc, features) =>
        RDDHash(doc, wmh.value.hash(FeaturesHash.toBagOfFeatures(features.map(_._2), docFreq)))
      }
    tfIdf
  }

  /**
    * Create WeightedMinHash instance and broadcasts it
    *
    * create it only once and keep on node
    * because the instance is relatively huge (3 * N of features * sampleSize(160 or 256 depends on mode) * 4)
    * On my system with 1.000.000 tokens it allocated 1920009680 bytes (~2Gb) (measured using JAMM)
    *
    * @param tokens     number of features
    * @param sampleSize depends on hashing mode and threshold
    * @return
    */
  def makeBroadcastedWmh(tokens: Int, sampleSize: Int): Broadcast[WeightedMinHash] = {
    val wmh = FeaturesHash.initWmh(tokens, sampleSize)
    session.sparkContext.broadcast(wmh)
  }

  protected def saveDocFreqToDB(docFreq: OrderedDocFreq, keyspace: String, tables: Tables): Unit = {
    log.warn(s"save document frequencies to DB")

    CassandraConnector(session.sparkContext).withSessionDo { cassandra =>
      val docsCols = tables.featuresDocsCols
      cassandra.execute(
        s"INSERT INTO $keyspace.${tables.featuresDocs} (${docsCols.id}, ${docsCols.docs}) VALUES (?, ?)",
        mode, int2Integer(docFreq.docs)
      )

      val freqCols = tables.featuresFreqCols
      val prepared = cassandra.prepare(s"INSERT INTO $keyspace.${tables.featuresFreq}" +
        s"(${freqCols.id}, ${freqCols.feature}, ${freqCols.weight}) VALUES (?, ?, ?)")

      docFreq.df.foreach { case(feature, weight) =>
        cassandra.execute(prepared.bind(mode, feature, int2Integer(weight)))
      }
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

    val FeaturesHashOpts(_, htnum, bandSize) = mode match {
      case Gemini.fileSimilarityMode => FeaturesHash.fileParams
      case Gemini.funcSimilarityMode => FeaturesHash.funcParams
    }

    val cols = tables.hashtablesCols
    rdd
      .flatMap { case RDDHash(doc, wmh) =>
        FeaturesHash.wmhToBands(wmh, htnum, bandSize).zipWithIndex.map { case (band, i) => (doc, i, band) }
      }
      .toDF(cols.sha, cols.hashtable, cols.value)
      .write
      .mode("append")
      .cassandraFormat(tables.hashtables(mode), keyspace)
      .save()
  }

  def mapAccumulator(sc: SparkContext, name: String): MapAccumulator = {
    val acc = new MapAccumulator
    sc.register(acc, name)
    acc
  }
}

object Hash {
  /**
    * Extracts functions from UAST
    * https://github.com/src-d/ml/blob/7ceecc659648914335a8f375714c35c31b8a9e8f/sourced/ml/transformers/moder.py#L84
    *
    * @param uast
    * @return (function name, UAST)
    */
  def extractFunctions(uast: Node): Iterable[(String, Node)] = {
    val FuncXpath = "//*[@roleFunction and @roleDeclaration]"
    val FuncNameXpath = "/*[@roleFunction and @roleIdentifier and @roleName] " +
      "| /*/*[@roleFunction and @roleIdentifier and @roleName]"

    val nested = mutable.Set[Node]()
    val allFuncs = BblfshClient.filter(uast, FuncXpath)

    allFuncs.foreach { func =>
      if (!nested.contains(func)) {
        BblfshClient.filter(func, FuncXpath).foreach { nestedFunc =>
          if (!func.equals(nestedFunc)) {
            nested.add(nestedFunc)
          }
        }
      }
    }

    allFuncs.flatMap { func =>
      if (!nested.contains(func)) {
        val name = BblfshClient.filter(func, FuncNameXpath).map(_.token).mkString("+")
        Iterable((name, func))
      } else {
        Iterable()
      }
    }
  }

  def apply(s: SparkSession, log: Slf4jLogger, mode: String = Gemini.fileSimilarityMode): Hash = new Hash(s, log, mode)
}
