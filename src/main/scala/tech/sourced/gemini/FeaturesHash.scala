package tech.sourced.gemini

import tech.sourced.featurext.generated.service.Feature
import tech.sourced.gemini.util.MathUtil

import scala.collection.Searching._

case class FeaturesHashOpts(sampleSize: Int, hashtablesNum: Int, bandSize: Int)

object FeaturesHash {
  // All params below must be the same for hash & query
  val defaultSeed = 0

  // Depend on threshold and sample_size
  // this values calculated for threshold=0.6456707644812872 and sample_size=160
  val fileParams = FeaturesHashOpts(sampleSize = 160, hashtablesNum = 20, bandSize = 8)
  // this values calculated for threshold=0.592 and sample_size=256
  val funcParams = FeaturesHashOpts(sampleSize = 256, hashtablesNum = 32, bandSize = 8)

  val defaultSampleSize = fileParams.sampleSize
  val defaultHashtablesNum = fileParams.hashtablesNum
  val defaultBandSize = fileParams.bandSize

  /**
    * Factory method for initializing WMH data structure
    * \w default parameters, specific to Gemini.
    *
    * Allocates at least 2*dim*sampleSize*4 bytes of RAM
    *
    * @param dim weight vector size
    * @param sampleSize number of samples
    * @param seed
    * @return
    */
  def initWmh(dim: Int, sampleSize: Int = defaultSampleSize, seed: Int = defaultSeed): WeightedMinHash = {
    val wmh = new WeightedMinHash(dim, sampleSize, seed)
    wmh
  }

  /**
    * Creates bag of features
    * It's array with a size of vocabulary
    * each element represents a feature weight as log-tf-log-idf
    *
    * similar code in apollo:
    * https://github.com/src-d/apollo/blob/bbb9f83fffc93e791fa59f064efadefa270f400a/apollo/hasher.py#L212
    *
    * @param features
    * @param docFreq
    * @return
    */
  def toBagOfFeatures(features: Iterator[Feature], docFreq: OrderedDocFreq): Array[Float] = {
    val OrderedDocFreq(docs, tokens, df) = docFreq

    val bag = new Array[Float](tokens.size)
    features.foreach { feature =>
      tokens.search(feature.name) match {
        case Found(idx) => {
          val tf = feature.weight

          bag(idx) = MathUtil.logTFlogIDF(tf, df(feature.name), docs)
        }
        case _ =>
      }
    }
    bag
  }

  /**
    * split hash to bands
    *
    * @param wmh
    * @param htnum
    * @param bandSize
    * @return
    */
  def wmhToBands(
                  wmh: Array[Array[Long]],
                  htnum: Int = defaultHashtablesNum,
                  bandSize: Int = defaultBandSize): Iterable[Array[Byte]] = {

    // a little bit verbose because java doesn't have uint32 type
    // in apollo it's one-liner:
    // https://github.com/src-d/apollo/blob/57e52394783d73e38cf1f862afc0166724991fd5/apollo/query.py#L35
    val bands = new Array[Array[Byte]](htnum)
    (0 until htnum).foreach { i =>
      val from = i * bandSize
      val band = new Array[Byte](bandSize*8)
      (0 until bandSize).foreach { j =>
        val z = from + j
        MathUtil.toUInt32ByteArray(wmh(z)(0)).copyToArray(band, j*8)
        MathUtil.toUInt32ByteArray(wmh(z)(1)).copyToArray(band, j*8 + 4)
      }
      bands(i) = band
    }
    bands
  }
}
