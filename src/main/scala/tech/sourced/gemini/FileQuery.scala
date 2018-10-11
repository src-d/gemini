package tech.sourced.gemini

import java.io.File
import java.nio.file.Files

import com.datastax.driver.core.{Session, SimpleStatement}
import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import org.bblfsh.client.BblfshClient
import org.slf4j.{Logger => Slf4jLogger}
import tech.sourced.featurext.FEClient
import tech.sourced.featurext.generated.service.Feature
import tech.sourced.featurext.generated.service.FeatureExtractorGrpc.FeatureExtractor
import tech.sourced.gemini.util.MathUtil

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

abstract class SimilarItem {
  def toString: String
}

case class SimilarFile(file: RepoFile) extends SimilarItem {
  override def toString: String = file.toString()
}
case class SimilarFunc(file: RepoFile, name: String, line: String) extends SimilarItem {
  override def toString: String = s"${file} function: ${name} line: ${line}"
}

/**
  * QueryResult contains iterators for full duplicates and similar files
  *
  * @param duplicates
  * @param similar
  */
case class QueryResult(duplicates: Iterable[RepoFile], similar: Iterable[SimilarItem])

/**
  * FileQuery queries similar and duplicated files
  *
  * @param conn db connection
  * @param bblfshClient
  * @param feClient
  * @param log
  * @param keyspace
  * @param tables
  */
class FileQuery(
  conn: Session,
  bblfshClient: BblfshClient,
  feClient: FeatureExtractor,
  docFreqPath: String = "",
  log: Slf4jLogger,
  keyspace: String,
  tables: Tables,
  mode: String
) {

  /**
    * find similar and duplicates for a file
    *
    * @param file
    * @return
    */
  def find(file: File): QueryResult = {
    val duplicates = findDuplicatesOfFile(file)
    val duplicatedShas = duplicates.map(_.sha).toSeq
    log.info(s"${duplicatedShas.length} duplicate SHA1s")

    val similarShas = findSimilarForFile(file)
      .map(_.split("@")(1)) // value for sha1 in Apollo hashtables is 'path@sha1', but we need only hashes
      .filterNot(sha => duplicatedShas.contains(sha))
    log.info(s"${similarShas.size} SHA1's found to be similar, after filtering duplicates")

    log.info("Looking up similar files metadata from DB")
    val similar = mode match {
      case Gemini.fileSimilarityMode => similarShas
        .flatMap(sha1 => Database.findFilesByHash(sha1, conn, keyspace, tables))
        .map(SimilarFile(_))
      case Gemini.funcSimilarityMode => similarShas.flatMap(item => {
        // item for functions is sha1_func_name:line
        val Array(sha1, rest) = item.split("_", 2)
        val Array(name, line) = rest.split(":")

        Database.findFilesByHash(sha1, conn, keyspace, tables).map(SimilarFunc(_, name, line))
      })
    }

    QueryResult(duplicates, similar)
  }

  protected def findDuplicatesOfFile(file: File): Iterable[RepoFile] = {
    Database.findFilesByHash(Gemini.computeSha1(file), conn, keyspace, tables)
  }

  protected def findSimilarForFile(file: File): Iterable[String] = {
    val docFreq :Option[OrderedDocFreq] = if (docFreqPath.isEmpty) {
      readDocFreqFromDB()
    } else {
      readDocFreqFromFile()
    }
    if (docFreq.isEmpty) {
      log.warn("Skip similarity query")
      Seq()
    } else {
      extractUAST(file) match {
        case Some(node) =>
          mode match {
            case Gemini.fileSimilarityMode => findSimilarItems(extractFeatures(node), docFreq.get)
            case Gemini.funcSimilarityMode => {
              Hash.extractFunctions(node).flatMap { case (fnName, fnUast) =>
                log.debug(s"looking for similar functions of function ${fnName}")
                val feats = FEClient.extract(fnUast, feClient, FEClient.funcLevelExtractors, log)
                findSimilarItems(feats, docFreq.get)
              }
            }
          }
        case _ => Seq()
      }
    }
  }

  private def findSimilarItems(featuresList: Iterable[Feature], docFreq: OrderedDocFreq): Seq[String] = {
    val FeaturesHashOpts(sampleSize, htnum, bandSize) = mode match {
      case Gemini.fileSimilarityMode => FeaturesHash.fileParams
      case Gemini.funcSimilarityMode => FeaturesHash.funcParams
    }

    val cols = tables.hashtablesCols
    val wmh = hashFile(featuresList, docFreq, sampleSize)

    val bands = FeaturesHash.wmhToBands(wmh, htnum, bandSize)

    log.info("Looking for similar items")
    val similar = bands.zipWithIndex.foldLeft(Set[String]()) { case (sim, (band, i)) =>
      val cql = s"""SELECT ${cols.sha} FROM $keyspace.${tables.hashtables}
        WHERE ${cols.hashtable}=$i AND ${cols.value}=0x${MathUtil.bytes2hex(band)}"""
      log.debug(cql)

      val sha1s = conn.execute(new SimpleStatement(cql))
        .asScala
        .map(_.getString("sha1"))

      sim ++ sha1s
    }
    log.info(s"Fetched ${similar.size} items from DB")

    similar.toSeq
  }

  protected def extractUAST(file: File): Option[Node] = {
    val byteArray = Files.readAllBytes(file.toPath)
    log.info(s"Extracting UAST")
    try {
      val resp = bblfshClient.parse(file.getName, new String(byteArray))
      if (resp.errors.nonEmpty) {
        val errors = resp.errors.mkString(",")
        log.error(s"bblfsh errors: ${errors}")
      }

      resp.uast
    } catch {
      case NonFatal(e) => {
        log.error(s"bblfsh error: ${e.toString}")
        None
      }
    }
  }

  protected def extractFeatures(uast: Node): Iterable[Feature] = {
    log.debug(s"uast received: ${uast.toString}")
    val result = FEClient.extract(uast, feClient, FEClient.fileLevelExtractors, log)
    log.debug(s"features: ${result.toString}")
    result
  }

  protected def readDocFreqFromDB(): Option[OrderedDocFreq] = {
    log.info(s"Reading docFreq from DB")
    val cols = tables.docFreqCols
    val row = conn.execute(s"SELECT * FROM ${tables.docFreq} WHERE ${cols.id} = '1'").one()
    if (row == null) {
      log.warn("Document frequency table is empty.")
      None
    } else {
      val df = row
        .getMap("df", classOf[java.lang.String], classOf[java.lang.Integer])
        .asScala
        .mapValues(_.toInt)

      Some(OrderedDocFreq(row.getInt(cols.docs), df.keys.toIndexedSeq, df))
    }
  }

  protected def readDocFreqFromFile(): Option[OrderedDocFreq] = {
    val docFreqFile = new File(docFreqPath)
    if (!docFreqFile.exists()) {
      log.warn("Document frequency for weighted min hash wasn't provided.")
      None
    } else {
      Some(OrderedDocFreq.fromJson(docFreqFile))
    }
  }

  protected def hashFile(features: Iterable[Feature], docFreq: OrderedDocFreq, sampleSize: Int): Array[Array[Long]] = {
    log.info(s"Initialize WMH")
    val wmh = FeaturesHash.initWmh(docFreq.tokens.size, sampleSize)

    log.info("Started hashing a file")
    val bag = FeaturesHash.toBagOfFeatures(features.iterator, docFreq)
    val hash = wmh.hash(bag)
    log.info("Finished hashing a file")
    hash
  }
}
