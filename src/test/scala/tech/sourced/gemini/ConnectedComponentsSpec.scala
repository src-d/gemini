package tech.sourced.gemini

import java.nio.ByteBuffer

import org.slf4j.{Logger => Slf4jLogger}
import org.scalatest.{FlatSpec, Matchers}
import tech.sourced.gemini.util.Logger

import scala.collection.mutable.ListBuffer

class TestConnectedComponents(log: Slf4jLogger) extends ConnectedComponents(log) {
  def getHashtables(): List[Byte] = List(0, 1, 2)

  def intToHash(x: Byte): ByteBuffer = ByteBuffer.wrap(Array[Byte](x))

  // emulate database, restrictions:
  // - each hashtable must have all elements
  // - results must be sorted by (hash values, key)
  def getHashValues(hashtable: Byte): Iterable[FileHash] = {
    hashtable match {
      // bucket for a&b and d&3
      case 0 => List(
        FileHash("a", intToHash(1)),
        FileHash("b", intToHash(1)),
        FileHash("c", intToHash(2)),
        FileHash("d", intToHash(3)),
        FileHash("e", intToHash(3))
      )
      // bucket for b&c
      case 1 => List(
        FileHash("a", intToHash(1)),
        FileHash("b", intToHash(2)),
        FileHash("c", intToHash(2)),
        FileHash("d", intToHash(3)),
        FileHash("e", intToHash(4))
      )
      // no bucket
      case 2 => List(
        FileHash("a", intToHash(1)),
        FileHash("b", intToHash(2)),
        FileHash("c", intToHash(3)),
        FileHash("d", intToHash(4)),
        FileHash("e", intToHash(5))
      )
    }
  }
}

class ConnectedComponentsSpec extends FlatSpec
  with Matchers {

    val logger = Logger("gemini")
    val cc = new TestConnectedComponents(logger)

    "makeBuckets" should "correctly create buckets" in {
      cc.makeBuckets()._1 shouldEqual List[List[Int]](
        // buckets from hashtable 0
        List(0, 1),
        List(3, 4),
        // bucket from hashtable 1
        List(1, 2)
      )
    }

    "elementsToBuckets" should "create correct map" in {
      val (buckets, _) = cc.makeBuckets()
      cc.elementsToBuckets(buckets) shouldEqual Map[Int, List[Int]](
        0 -> List(0),
        1 -> List(0, 2),
        2 -> List(2),
        3 -> List(1),
        4 -> List(1)
      )
    }

  "findInBuckets" should "return connected components" in {
    val (buckets, _) = cc.makeBuckets()
    val elementToBuckets = cc.elementsToBuckets(buckets)
    cc.findInBuckets(buckets, elementToBuckets) shouldEqual Map[Int, Set[Int]](
      0 -> Set(1, 2, 0),
      1 -> Set(3, 4)
    )
  }
}

