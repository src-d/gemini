package tech.sourced.gemini.util

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

class MapAccumulator extends AccumulatorV2[(String, Int), Map[String, Int]] {

  private val underlyingMap: mutable.HashMap[String, Int] = mutable.HashMap.empty
  override def isZero: Boolean = underlyingMap.isEmpty

  override def copy(): AccumulatorV2[(String, Int), Map[String, Int]] = {
    val newMapAccumulator = new MapAccumulator()
    underlyingMap.foreach(newMapAccumulator.add)
    newMapAccumulator
  }

  override def reset(): Unit = underlyingMap.clear

  override def value: Map[String, Int] = underlyingMap.toMap

  override def add(kv: (String, Int)): Unit = {
    val (k, v) = kv
    underlyingMap += k -> (underlyingMap.getOrElse(k, 0) + v)
  }

  override def merge(other: AccumulatorV2[(String, Int), Map[String, Int]]): Unit =
    other match {
      case map: MapAccumulator =>
        map.value.foreach(this.add)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
}
