package tech.sourced.gemini

import java.nio.ByteBuffer
import org.slf4j.{Logger => Slf4jLogger}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class TestConnectedComponents(log: Slf4jLogger) extends ConnectedComponents(log) {
  def getHashtables(): List[Byte] = List(0, 1, 2)

  def getHashValues(hashtable: Byte): Iterable[FileHash] = {
    val uniqVal = hashtable + 10
    val intersectVal = hashtable + 1

    List(
      // same elem in all buckets
      FileHash("a", ByteBuffer.wrap(Array[Byte](1))),
      FileHash("b" + hashtable.toString, ByteBuffer.wrap(Array[Byte](1))),
      // different elem in each bucket
      FileHash("c" + hashtable.toString, ByteBuffer.wrap(Array[Byte](uniqVal.toByte))),
      // appear in 2 buckets
      FileHash("d", ByteBuffer.wrap(Array[Byte](intersectVal.toByte))),
      FileHash("d" + hashtable.toString, ByteBuffer.wrap(Array[Byte](intersectVal.toByte)))
    )
  }
}

class ConnectedComponentsSpec extends FlatSpec
  with Matchers {

    val logger = Logger("gemini")
    val cc = new TestConnectedComponents(logger)

    "makeBuckets" should "correctly create buckets" in {
      cc.makeBuckets()._1 shouldEqual List[List[Int]](
        List(0, 1),
        List(2),
        List(3, 4),
        List(0, 5),
        List(6),
        List(3, 7),
        List(0, 8),
        List(9),
        List(3, 10)
      )
    }

    "elementsToBuckets" should "create correct map" in {
      val (buckets, _) = cc.makeBuckets()
      cc.elementsToBuckets(buckets) shouldEqual Map[Int, List[Int]](
        0 -> List(0, 3, 6),
        1 -> List(0),
        2 -> List(1),
        3 -> List(2, 5, 8),
        4 -> List(2),
        5 -> List(3),
        6 -> List(4),
        7 -> List(5),
        8 -> List(6),
        9 -> List(7),
        10 -> List(8)
      )
    }

  "findInBuckets" should "return connected components" in {
    val (buckets, _) = cc.makeBuckets()
    val elementToBuckets = cc.elementsToBuckets(buckets)
    cc.findInBuckets(buckets, elementToBuckets) shouldEqual Map[Int, Set[Int]](
      0 -> Set(3, 10, 7, 4),
      1 -> Set(9),
      2 -> Set(0, 8, 5, 1),
      3 -> Set(6),
      4 -> Set(2)
    )
  }
}

