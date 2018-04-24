package tech.sourced.gemini

import java.io.{File, PrintWriter}

import scala.util.parsing.json.{JSONArray, JSONObject}

/**
  * Ordered document frequency
  *
  * @param docs
  * @param tokens
  * @param df
  */
case class OrderedDocFreq(docs: Int, tokens: Array[String], df: Map[String, Int]) {
  def saveToJson(filename: String = OrderedDocFreq.defaultFile): Unit = {
    val jsonObj = JSONObject(Map[String, Any](
      "docs" -> docs,
      "tokens" -> JSONArray(tokens.toList),
      "df" -> JSONObject(df)
    ))
    val w = new PrintWriter(filename)
    w.write(jsonObj.toString())
    w.close()
  }
}

object OrderedDocFreq {
  val defaultFile: String = "docfreq.json"

  def fromJson(file: File): OrderedDocFreq = {
    val docFreqMap = JSONUtils.parseFile[Map[_, _]](file)
      .asInstanceOf[Map[String, Any]]

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

    new OrderedDocFreq(docs, tokens.toArray, df)
  }
}

