package tech.sourced.gemini

import org.scalatest.{FlatSpec, Matchers}
import scala.io.Source
import org.scalatest.Tag

// Tag to set which tests depend on pyhton
object PythonDep extends Tag("tech.sourced.tags.PythonDep")

class WeightedMinHashSpec extends FlatSpec with Matchers {

  "WeightedMinHash constructor" should "initialize correctly" taggedAs(PythonDep) in {
    val mg = new WeightedMinHash(2, 4, 1)

    mg.rs.length should be(4)
    mg.lnCs.length should be(4)
    mg.betas.length should be(4)
    mg.sampleSize should be(4)
  }

  def readCSV(filename: String): Array[Array[Float]] = {
    Source
      .fromFile(s"src/test/resources/weighted-minhash/csv/${filename}")
      .getLines()
      .map(_.split(",").map(_.trim.toFloat))
      .toArray
  }

  "WeightedMinHash hash" should "hash tiny data" taggedAs(PythonDep) in {
    val input = readCSV("tiny-data.csv")

    val rs = readCSV("tiny-rs.csv")
    val lnCs = readCSV("tiny-ln_cs.csv")
    val betas = readCSV("tiny-betas.csv")

    input.zipWithIndex.foreach {
      case (v, i) =>
        val wmh = new WeightedMinHash(v.length, 128, rs, lnCs, betas)
        val hashes = wmh.hash(v)
        val realHashes = readCSV(s"tiny-hashes-${i}.csv").map(_.map(_.toLong))

        hashes should be(realHashes)
    }
  }

  "WeightedMinHash hash" should "hash big data" taggedAs(PythonDep) in {
    val input = readCSV("big-data.csv")

    val rs = readCSV("big-rs.csv")
    val lnCs = readCSV("big-ln_cs.csv")
    val betas = readCSV("big-betas.csv")

    input.zipWithIndex.foreach {
      case (v, i) =>
        val wmh = new WeightedMinHash(v.length, 128, rs, lnCs, betas)
        val hashes = wmh.hash(v)
        val realHashes = readCSV(s"big-hashes-${i}.csv").map(_.map(_.toLong))

        hashes should be(realHashes)
    }
  }
}
