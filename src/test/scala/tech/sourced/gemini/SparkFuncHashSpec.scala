package tech.sourced.gemini

import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

@tags.Bblfsh
@tags.FeatureExtractor
@tags.Spark
class SparkFuncHashSpec extends FlatSpec
  with Matchers
  with BaseSparkSpec
  with Logging
  with BeforeAndAfterAll {

  useDefaultSparkConf()

  // files for hash test
  val filePaths = Array(
    "funcs.go"
  )

  def hashWithNewGemini(): HashResult = {
    val gemini = Gemini(sparkSession)
    val hash = LimitedHash(sparkSession, log, Gemini.funcSimilarityMode, filePaths)
    val repos = gemini.getRepos(s"src/test/resources/siva/duplicate-funcs")
    hash.forRepos(repos)
  }

  "Hash" should "return correct number of functions" in {
    val hashResult = hashWithNewGemini()
    val funcs = hashResult.hashes
    val fCount = funcs.count()
    fCount shouldEqual 2
  }

  "Hash" should "generate docFreq in func mode" in {
    val docFreq = hashWithNewGemini().docFreq
    docFreq.docs shouldEqual 2
    docFreq.tokens.size shouldEqual 66
  }

}
