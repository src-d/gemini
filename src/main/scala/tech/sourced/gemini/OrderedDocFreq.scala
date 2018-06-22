package tech.sourced.gemini

import java.io.{File, PrintWriter}

import scala.io.Source

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
  * Ordered document frequency
  *
  * @param docs
  * @param tokens
  * @param df
  */
case class OrderedDocFreq(docs: Int, tokens: IndexedSeq[String], df: collection.Map[String, Int]) {
  def saveToJson(filename: String): Unit = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val out = new PrintWriter(filename)
    mapper.writeValue(out, Map(
      "docs" -> docs,
      "tokens" -> tokens,
      "df" -> df
    ))
    out.close()
  }
}

object OrderedDocFreq {
  def fromJson(file: File): OrderedDocFreq = {
    val docFreqMap = parseFile[Map[String, Any]](file)
    val docs = docFreqMap.get("docs") match {
      case Some(v) => v.asInstanceOf[Int]
      case None => throw new RuntimeException(s"Can not parse key 'docs' in docFreq:${file.getAbsolutePath}")
    }
    val df = docFreqMap.get("df") match {
      case Some(v) => v.asInstanceOf[Map[String, Int]]
      case None => throw new RuntimeException(s"Can not parse key 'df' in docFreq:${file.getAbsolutePath}")
    }
    val tokens = docFreqMap.get("tokens") match {
      case Some(v) => v.asInstanceOf[List[String]].toArray
      case None => throw new RuntimeException(s"Can not parse key 'tokens' in docFreq:${file.getAbsolutePath}")
    }
    OrderedDocFreq(docs, tokens, df)
  }

  def parseFile[T: Manifest](file: File): T = {
    val json = Source.fromFile(file)
    val mapper = new ObjectMapper with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    mapper.readValue[T](json.reader)
  }
}

