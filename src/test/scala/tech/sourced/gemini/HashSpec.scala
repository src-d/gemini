package tech.sourced.gemini

import org.apache.spark.internal.Logging
import org.scalatest.{FlatSpec, Matchers}

@tags.Spark
class HashSpec extends FlatSpec
  with Matchers
  with BaseSparkSpec
  with Logging {

  val i = "i.*"
  val g = "g.*"
  val k = "k.*"

  val defaultConf = useDefaultSparkConf()
  val docFreq = OrderedDocFreq(docs = 2, tokens = Array(i, g, k), df = Map(i -> 2, g -> 2, k -> 1))

  "hashFeatures" should "RDD should be same to DF" in {
    val hasher = Hash(sparkSession, log)

    val features = List(
      (i, "file/1", 1L),
      (i, "file/2", 1L),
      (g, "file/1", 2L),
      (g, "file/2", 2L),
      (k, "file/2", 3L)
    )

    val featuresDf = sparkSession.createDataFrame(features).toDF("feature", "doc", "weight")

    val df = hasher.hashFeaturesDF(docFreq, featuresDf).collect()
    val rdd = hasher.hashFeaturesRDD(docFreq, featuresDf).collect()

    rdd.length should be equals (df.length)
    rdd.map(_.doc) should contain theSameElementsInOrderAs df.map(_.doc)
    rdd.map(_.wmh) should be equals df.map(_.wmh)
  }

}
