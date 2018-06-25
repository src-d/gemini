package tech.sourced.gemini

import org.scalatest.{FlatSpec, Matchers}
import tech.sourced.gemini.util.MathUtil

class MathUtilSpec extends FlatSpec
  with Matchers {

  "toUInt32ByteArray" should "create correct arrays" in {
    MathUtil.toUInt32ByteArray(0) shouldEqual Array[Byte](0, 0, 0, 0)
    MathUtil.toUInt32ByteArray(1) shouldEqual Array[Byte](1, 0, 0, 0)
    MathUtil.toUInt32ByteArray(12345) shouldEqual Array[Byte](57, 48, 0, 0)
    // max uint32
    MathUtil.toUInt32ByteArray(4294967295L) shouldEqual Array[Byte](-1, -1, -1, -1)
    // overflow
    MathUtil.toUInt32ByteArray(4294967296L) shouldEqual Array[Byte](0, 0, 0, 0)
    MathUtil.toUInt32ByteArray(4294967297L) shouldEqual Array[Byte](1, 0, 0, 0)
    MathUtil.toUInt32ByteArray(-1) shouldEqual Array[Byte](-1, -1, -1, -1)
  }

}
