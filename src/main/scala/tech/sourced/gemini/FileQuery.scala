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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/**
  * QueryResult contains iterators for full duplicates and similar files
  *
  * @param duplicates
  * @param similar
  */
case class QueryResult(duplicates: Iterable[RepoFile], similar: Iterable[RepoFile])

/**
  * FileQuery queries similar and duplicated files
  *
  * @param conn db connection
  * @param bblfshClient
  * @param feClient
  * @param docFreqPath will be replaced by database in future
  * @param log
  * @param keyspace
  * @param tables
  */
class FileQuery(conn: Session,
            bblfshClient: BblfshClient,
            feClient: FeatureExtractor,
            docFreqPath: String = "",
            log: Slf4jLogger,
            keyspace: String,
            tables: Tables) {

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
    log.info(s"${similarShas.length} SHA1's found to be similar, after filtering duplicates")

    val similar = similarShas.flatMap(sha1 => Database.findFilesByHash(sha1, conn, keyspace, tables))
    log.info("Looking up similar files metadata from DB")
    QueryResult(duplicates, similar)
  }

  protected def findDuplicatesOfFile(file: File): Iterable[RepoFile] = {
    Database.findFilesByHash(Gemini.computeSha1(file), conn, keyspace, tables)
  }

  protected def findSimilarForFile(file: File): Seq[String] = {
    val docFreqFile = new File(docFreqPath)
    if (!docFreqFile.exists()) {
      log.warn("Document frequency for weighted min hash wasn't provided. Skip similarity query")
      Seq()
    } else {
      extractUAST(file) match {
        case Some(node) =>
          val featuresList = extractFeatures(node)
          findSimilarFiles(featuresList, docFreqFile)
        case _ => Seq()
      }
    }
  }

  private def findSimilarFiles(featuresList: Iterable[Feature], docFreqFile: File): Seq[String] = {
    if (featuresList.isEmpty) {
      log.warn(s"file: '${docFreqFile.getPath}' doesn't contain features")
      Seq()
    } else {
      val cols = tables.hashtablesCols
      val wmh = hashFile(featuresList, docFreqFile)

      val bands = FeaturesHash.wmhToBands(wmh)

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
    val result = FEClient.extract(uast, feClient, log)
    log.debug(s"features: ${result.toString}")
    result
  }

  protected def hashFile(features: Iterable[Feature], docFreqFile: File): Array[Array[Long]] = {
    log.info(s"Reading docFreq from ${docFreqFile.getAbsolutePath}")
    val docFreq = OrderedDocFreq.fromJson(docFreqFile)

    log.info(s"Initialize WMH for ")
    val wmh = FeaturesHash.initWmh(docFreq.tokens.size)

    log.info("Started hashing a file")
    val bag = FeaturesHash.toBagOfFeatures(features, docFreq)
    val hash = wmh.hash(bag)
    log.info("Finished hashing a file")
    hash
  }
}
