package tech.sourced.gemini.util

object MathUtil {
  private val maxUint32 = 4294967295L

  def bytes2hex(bytes: Array[Byte], sep: Option[String] = None): String = {
    sep match {
      case None => bytes.map("%02x".format(_)).mkString
      case Some(s) => bytes.map("%02x".format(_)).mkString(s)
    }
  }

  /**
    * Converts Long value to byte array of uint32 type
    * little-endian as in numpy
    *
    * @param value
    * @return
    */
  def toUInt32ByteArray(value: Long): Array[Byte] = {
    Array[Byte](
      value.toByte,
      (value >> 8).toByte,
      (value >> 16).toByte,
      (value >> 24).toByte
    )
  }

  def logTFlogIDF(tf: Double, df: Double, ndocs: Int): Double = math.log(1 + tf) * math.log(ndocs / df)
}
