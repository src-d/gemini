package tech.sourced.gemini

import java.io.{File, PrintWriter}
import scala.collection.immutable

import scala.util.parsing.json.{JSONArray, JSONObject}

/**
  * Ordered document frequency
  *
  * @param docs
  * @param tokens
  * @param df
  */
case class OrderedDocFreq(docs: Int, tokens: immutable.List[String], df: immutable.Map[String, Int]) {
  def saveToJson(filename: String): Unit = {
    val jsonObj = JSONObject(immutable.Map[String, Any](
      "docs" -> docs,
      "tokens" -> JSONArray(tokens),
      "df" -> JSONObject(df)
    ))
    val w = new PrintWriter(filename)
    w.write(jsonObj.toString())
    w.close()
  }
}

object OrderedDocFreq {
  def fromJson(file: File): OrderedDocFreq = {
    val docFreqMap = JSONUtils.parseFile[Map[String, Any]](file)

    val docs = docFreqMap.get("docs") match {
      case Some(v) => v.asInstanceOf[Double].toInt
      case None => throw new Exception("can not parse docs in docFreq")
    }
    val df = docFreqMap.get("df") match {
      case Some(v) => v.asInstanceOf[Map[String, Double]].mapValues(_.toInt)
      case None => throw new Exception("can not parse df in docFreq")
    }
    val tokens = docFreqMap.get("tokens") match {
      case Some(v) => v.asInstanceOf[List[String]]
      case None => throw new Exception("can not parse tokens in docFreq")
    }

    OrderedDocFreq(docs, tokens, df)
  }
}

