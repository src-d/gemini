package tech.sourced.gemini

import tech.sourced.featurext.generated.service.Feature

import scala.collection.mutable

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
                    seed: Int = defaultSeed): Array[Array[Long]] = {
    val OrderedDocFreq(docs, tokens, df) = docFreq

    if (wmh == null || tokensSize != tokens.size) {
      wmh = new WeightedMinHash(tokens.size, sampleSize, seed)
    }

    val bag = mutable.ArrayBuffer.fill(tokens.size)(0.toDouble)
    features.foreach { feature =>
      val idx = tokens.indexOf(feature.name)
      idx match {
        case idx if idx >= 0 => {
          val tf = feature.weight.toDouble

          bag(idx) = MathUtil.logTFlogIDF(tf, df(feature.name), docs)
        }
        case _ =>
      }
    }
    wmh.hash(bag.toArray)
  }

  // split hash to bands
  // a little bit verbose because java doesn't have uint32 type
  // in apollo it's one-liner:
  // https://github.com/src-d/apollo/blob/57e52394783d73e38cf1f862afc0166724991fd5/apollo/query.py#L35
  def wmhToBands(
                  wmh: Array[Array[Long]],
                  htnum: Int = defaultHashtablesNum,
                  bandSize: Int = defaultBandSize): Iterable[Array[Byte]] = {
    (0 until htnum).map { i =>
      val from = i * bandSize
      val to = (i + 1) * bandSize
      val band = (from until to).foldLeft(Array.empty[Byte]) { (arr, j) =>
        arr ++ MathUtil.toUInt32ByteArray(wmh(j)(0)) ++ MathUtil.toUInt32ByteArray(wmh(j)(1))
      }
      band
    }
  }
}
