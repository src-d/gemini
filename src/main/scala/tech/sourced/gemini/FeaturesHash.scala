package tech.sourced.gemini

import tech.sourced.featurext.generated.service.Feature

import scala.collection.mutable
import scala.collection.Searching._

object FeaturesHash {
  // All params below must be the same for hash & query
  val defaultSeed = 0
  val defaultSampleSize = 128
  // Depend on threshold and sample_size
  // this values calculated for threshold=0.8 and sample_size=128
  val defaultHashtablesNum = 9
  val defaultBandSize = 13

  private var tokensSize: Int = _
  private var wmh: WeightedMinHash = _

  def hashFeatures(
                    docFreq: OrderedDocFreq,
                    features: Iterable[Feature],
                    sampleSize: Int = defaultSampleSize,
                    seed: Int = defaultSeed): Array[Array[Long]] = synchronized {
    // keep created WeightedMinHash instance cause the calculation of parameters is expensive for large dimension
    if (wmh == null || tokensSize != docFreq.tokens.size) {
      wmh = new WeightedMinHash(docFreq.tokens.size, sampleSize, seed)
      tokensSize = docFreq.tokens.size
    }

    val bag = toBagOfFeatures(features, docFreq)
    wmh.hash(bag)
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
  private def toBagOfFeatures(features: Iterable[Feature], docFreq: OrderedDocFreq): Array[Double] = {
    val OrderedDocFreq(docs, tokens, df) = docFreq

    val bag = new Array[Double](tokens.size)//mutable.ArrayBuffer.fill(tokens.size)(0.toDouble)
    features.foreach { feature =>
      tokens.search(feature.name) match {
        case Found(idx) => {
          val tf = feature.weight.toDouble

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
