package tech.sourced.gemini

import java.io.File
import java.nio.file.Files

import scala.reflect.ClassTag
import scala.util.parsing.json.JSON

/**
  * Temporary helper while we have to support compatibility with apollo
  */
object JSONUtils {
  def parseFile[T: ClassTag](file: File): T = {
    val paramsByteArray = Files.readAllBytes(file.toPath)
    mustParse[T](new String(paramsByteArray))
  }

  def mustParse[T: ClassTag](input: String): T = {
    JSON.parseFull(input) match {
      case Some(res: T) => res
      case Some(_) => throw new Exception("incorrect json")
      case None => throw new Exception("can't parse json")
    }
  }
}
