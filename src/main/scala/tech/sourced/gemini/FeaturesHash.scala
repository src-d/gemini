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
    val OrderedDocFreq(docs, tokens, df) = docFreq

    if (wmh == null || tokensSize != tokens.size) {
      wmh = new WeightedMinHash(tokens.size, sampleSize, seed)
      tokensSize = tokens.size
    }

    val bag = mutable.ArrayBuffer.fill(tokens.size)(0.toDouble)
    features.foreach { feature =>
      tokens.search(feature.name) match {
        case Found(idx) => {
          val tf = feature.weight.toDouble

          bag(idx) = MathUtil.logTFlogIDF(tf, df(feature.name), docs)
        }
        case _ =>
      }
    }
    wmh.hash(bag.toArray)
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
