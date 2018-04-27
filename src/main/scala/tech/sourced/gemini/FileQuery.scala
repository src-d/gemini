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

    val similarShas = findSimilarForFile(file, conn, bblfshClient, feClient, docFreqPath)
      .map(_.split("@")(1)) // value for sha1 in Apollo hashtables is 'path@sha1', but we need only hashes
      .filterNot(sha => duplicatedShas.contains(sha))

    log.info(s"${similarShas.length} SHA1's found to be similar")

    val similar = similarShas.flatMap(sha1 => Database.repoFilesByHash(sha1, conn, keyspace, tables))
    QueryResult(duplicates, similar)
  }

  protected def findDuplicatesOfFile(file: File): Iterable[RepoFile] = {
    Database.repoFilesByHash(Gemini.computeSha1(file), conn, keyspace, tables)
  }

  protected def findSimilarForFile(
                          file: File,
                          conn: Session,
                          bblfshClient: BblfshClient,
                          feClient: FeatureExtractor,
                          docFreqPath: String): Seq[String] = {
    val docFreqFile = new File(docFreqPath)
    if (!docFreqFile.exists()) {
      log.warn("Document frequency for weighted min hash wasn't provided. Skip similarity query")
      Seq()
    } else {
      extractUAST(file, bblfshClient) match {
        case Some(node) =>
          val featuresList = extractFeatures(feClient, node)
          findSimilarFiles(featuresList, conn, docFreqFile)
        case _ => Seq()
      }
    }
  }

  private def findSimilarFiles(featuresList: Iterable[Feature], conn: Session, docFreqFile: File): Seq[String] = {
    if (featuresList.isEmpty) {
      log.warn("file doesn't contain features")
      Seq()
    } else {
      val cols = tables.hashtablesCols
      val wmh = hashFile(featuresList, docFreqFile)

      val bands = FeaturesHash.wmhToBands(wmh)

      log.info("Looking for similar items")
      val similar = bands.zipWithIndex.foldLeft(Set[String]()) { case (sim, (band, i)) =>
        val table = "hashtables"
        val cql = s"""SELECT ${cols.sha} FROM $keyspace.$table
          WHERE ${cols.hashtable}=$i AND ${cols.value}=0x${MathUtil.bytes2hex(band)}"""
        log.debug(cql)

        val sha1s = conn.execute(new SimpleStatement(cql))
          .asScala
          .map(_.getString("sha1"))

        sim ++ sha1s
      }
      log.info(s"Fetched ${similar.size} items")

      similar.toSeq
    }
  }

  protected def extractUAST(file: File, client: BblfshClient): Option[Node] = {
    val byteArray = Files.readAllBytes(file.toPath)
    log.info(s"Extracting UAST")
    try {
      val resp = client.parse(file.getName, new String(byteArray))
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

  protected def extractFeatures(client: FeatureExtractor, uast: Node): Iterable[Feature] = {
    log.debug(s"uast received: ${uast.toString}")
    val result = FEClient.extract(uast, client, log)
    log.debug(s"features: ${result.toString}")
    result
  }

  protected def hashFile(features: Iterable[Feature], docFreqFile: File): Array[Array[Long]] = {
    log.info("Reading docFreq")
    val docFreq = OrderedDocFreq.fromJson(docFreqFile)

    log.info("Started hashing file")
    val hash = FeaturesHash.hashFeatures(docFreq, features)

    log.info("Finished hashing file")
    hash
  }
}
