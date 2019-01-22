package tech.sourced.gemini

import java.io.File

import com.datastax.driver.core.{Session, SimpleStatement}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter}
import org.slf4j.{Logger => Slf4jLogger}

import scala.collection.JavaConverters._
import scala.sys.process._

/**
  * ReportResult contains iterators for full duplicates and similar files
  *
  * @param duplicates
  * @param similar
  */
case class ReportResult(duplicates: ReportDuplicates, similar: Iterable[Iterable[SimilarItem]])

sealed abstract class ReportDuplicates(v: Iterable[Any]) {
  def empty(): Boolean = {
    v.isEmpty
  }

  def size(): Int = v.size
}

case class ReportByLine(v: Iterable[RepoFile]) extends ReportDuplicates(v)

case class ReportExpandedGroup(v: Iterable[Iterable[RepoFile]]) extends ReportDuplicates(v)

class Report(conn: Session, log: Slf4jLogger, keyspace: String, tables: Tables) {

  /**
    * Finds duplicate files among hashed repositories
    * It is used one query per unique duplicate file, plus an extra one
    * (Only supported by Apache Cassandra databases)
    *
    * @return
    */
  def reportCassandraGroupBy(): Iterable[Iterable[RepoFile]] = {
    findAllDuplicateBlobHashes()
      .map { item =>
        Database.findFilesByHash(item.sha, conn, keyspace, tables)
      }
  }

  def reportCommunities(communities: List[(Int, List[Int])],
                        elementIds: Map[String, Int],
                        mode: String): Iterable[Iterable[SimilarItem]] = {
    log.info(s"Report similar items from DB $keyspace")
    val sim = getCommunities(communities, elementIds, mode).toSeq
    log.info(s"${sim.length} similar communities")
    sim
  }

  /**
    * Return connected components from DB hashtables
    *
    * @return - Map of connected components groupId to list of elements
    *         - Map of element ids to list of bucket indices
    *         - Map of element to ID
    */
  def findConnectedComponents(mode: String): (Map[Int, Set[Int]], Map[Int, List[Int]], Map[String, Int]) = {
    log.info(s"Finding ${mode} connected components")
    val hashtablesTable = s"${tables.hashtables}_${mode}"
    val cc = new DBConnectedComponents(log, conn, hashtablesTable, keyspace)
    val (buckets, elementIds) = cc.makeBuckets()
    val elsToBuckets = cc.elementsToBuckets(buckets)

    val result = cc.findInBuckets(buckets, elsToBuckets)

    (result, elsToBuckets, elementIds)
  }

  /**
    * Finds the blob_hash that are repeated in the database, and how many times
    * (Only supported by Apache Cassandra databases)
    *
    * @return
    */
  def findAllDuplicateBlobHashes(): Iterable[DuplicateBlobHash] = {
    val hash = tables.metaCols.sha
    val dupCount = "count"
    val duplicatesCountCql = s"SELECT $hash, COUNT(*) as $dupCount FROM $keyspace.${tables.meta} GROUP BY $hash"
    conn
      .execute(new SimpleStatement(duplicatesCountCql))
      .asScala
      .filter(_.getLong(dupCount) > 1)
      .map { r =>
        DuplicateBlobHash(r.getString(tables.metaCols.sha), r.getLong(dupCount))
      }
  }

  /**
    * Finds the groups of duplicate files identified by the blob_hash
    *
    * @return
    */
  def findAllDuplicateItems(): Iterable[Iterable[RepoFile]] = {
    val hash = tables.metaCols.sha
    val distinctBlobHash = s"SELECT distinct $hash FROM $keyspace.${tables.meta}"

    conn
      .execute(new SimpleStatement(distinctBlobHash))
      .asScala
      .flatMap { r =>
        val dupes = Database.findFilesByHash(r.getString(hash), conn, keyspace, tables)
        if (dupes.size > 1) {
          List(dupes)
        } else {
          List()
        }
      }
  }

  case class funcElem(sha1: String, name: String, line: String)

  def getCommunities(communities: List[(Int, List[Int])],
                     elementIds: Map[String, Int],
                     mode: String): Iterable[Iterable[SimilarItem]] = {

    val idToElem = for ((elem, id) <- elementIds) yield (id, elem.split("@")(1))

    // Transform communities of element IDs to communities of sha1s
    // Equivalent to apollo graph.py BatchedCommunityResolver._gen_hashes
    // https://github.com/src-d/apollo/blob/f51c5a92c24cbedd54b9b30bab02f03e51fd27b3/apollo/graph.py#L295
    val communitiesElem = communities
      .map { case (_, community) =>
        community
          .filter(idToElem.contains)
          .map(idToElem)
      }
      .filter(_.size > 1)

    mode match {
      case Gemini.fileSimilarityMode => getSimilarFiles(communitiesElem)
      case Gemini.funcSimilarityMode => getSimilarFuncs(communitiesElem)
    }
  }

  def getSimilarFiles(communities: List[List[String]]): Iterable[Iterable[SimilarFile]] = {
    val cols = tables.metaCols

    communities.map(sha1s => {
      val elems = sha1s.map(st => s"'$st'").mkString(",")
      val query = s"select sha1, repo, commit, path from $keyspace.${tables.meta} where sha1 in ($elems)"

      conn
        .execute(new SimpleStatement(query))
        .asScala
        .map { row =>
          SimilarFile(RepoFile(row.getString(cols.repo), row.getString(cols.commit),
            row.getString(cols.path), row.getString(cols.sha)))
        }
    })
  }

  def getSimilarFuncs(communities: List[List[String]]): Iterable[Iterable[SimilarFunc]] = {
    val cols = tables.metaCols

    communities.map(community => {
      community.map { elem =>
        val (sha1, name, line) = Gemini.splitFuncItem(elem)
        funcElem(sha1, name, line)
      }
    }).map(community => {
      val elems = community.map(elem => s"'${elem.sha1}'").mkString(",")
      val query = s"select sha1, repo, commit, path from $keyspace.${tables.meta} where sha1 in ($elems)"

      val shaToFile = conn
        .execute(new SimpleStatement(query))
        .asScala
        .foldLeft(Map[String, RepoFile]()) { (result, row) =>
          val sha1 = row.getString(cols.sha)

          result + (sha1 ->
            RepoFile(row.getString(cols.repo), row.getString(cols.commit), row.getString(cols.path), sha1))
        }

      community.map { elem => SimilarFunc(shaToFile(elem.sha1), elem.name, elem.line)}
    })
  }


  def findSimilarItems(ccDirPath: String, mode: String): Iterable[Iterable[SimilarItem]] = {

    val (connectedComponents, elsToBuckets, elementIds) = findConnectedComponents(mode)
    saveConnectedComponents(connectedComponents, elsToBuckets, ccDirPath)

    log.info("Detecting communities in Python")
    val pythonCmd = s"python3 src/main/python/community-detector/report.py ${ccDirPath}"
    val rc = pythonCmd.!

    if (rc == 0) {
      val communities = readCommunities(ccDirPath)
      reportCommunities(communities, elementIds, mode)
    } else {
      log.error(s"Failed to execute '${pythonCmd}', skipping ${mode} similarity report")
      Iterable[Iterable[SimilarItem]]()
    }
  }

  def saveConnectedComponents(ccs: Map[Int, Set[Int]],
                              elsToBuckets: Map[Int, List[Int]],
                              outputPath: String): Unit = {
    log.info("Saving ConnectedComponents to Parquet")
    val schema = SchemaBuilder
      .record("ccs")
      .fields()
      .name("cc").`type`().intType().noDefault()
      .name("element_ids").`type`().array().items().intType().noDefault()
      .endRecord()

    // delete old file if exists
    val parquetFile = new File(s"$outputPath/cc.parquet")
    parquetFile.delete()

    // make it compatible with python
    val parquetConf = new Configuration()
    parquetConf.setBoolean("parquet.avro.write-old-list-structure", false)

    val parquetFilePath = new Path(s"$outputPath/cc.parquet")
    val writer = AvroParquetWriter.builder[GenericRecord](parquetFilePath)
      .withSchema(schema)
      .withConf(parquetConf)
      .build()

    ccs.foreach { case (cc, ids) =>
      val record = new GenericRecordBuilder(schema)
        .set("cc", cc)
        .set("element_ids", ids.toArray)
        .build()

      writer.write(record)
    }

    writer.close()

    log.info("Saving auxiliary data structure (id to buckets) to Parquet")
    val schemaBuckets = SchemaBuilder
      .record("id_to_buckets")
      .fields()
      .name("buckets").`type`().array().items().intType().noDefault()
      .endRecord()

    // delete old file if exists
    val parquetFileButckets = new File(s"$outputPath/buckets.parquet")
    parquetFileButckets.delete()

    val parquetBucketsFilePath = new Path(s"$outputPath/buckets.parquet")
    val writerBuckets = AvroParquetWriter.builder[GenericRecord](parquetBucketsFilePath)
      .withSchema(schemaBuckets)
      .withConf(parquetConf)
      .build()

    elsToBuckets.foreach { case (_, bucket) =>
      val record = new GenericRecordBuilder(schemaBuckets)
        .set("buckets", bucket.toArray)
        .build()

      writerBuckets.write(record)
    }

    writerBuckets.close()
  }

  def readCommunities(dirPath: String): List[(Int, List[Int])] = {
    val parquetFilePath = new Path(s"$dirPath/communities.parquet")
    log.info(s"Reading detected communities from $parquetFilePath")
    val reader = AvroParquetReader.builder[GenericRecord](parquetFilePath).build()

    Iterator
      .continually(reader.read)
      .takeWhile(_ != null)
      .map { record =>
        val communityId = record.get("community_id").asInstanceOf[Long].toInt

        val elementIds = record
          .get("element_ids")
          .asInstanceOf[java.util.ArrayList[GenericData.Record]]
          .asScala
          .toList
          .map(_.get("item").asInstanceOf[Long].toInt)

        (communityId, elementIds)
      }
      .toList
  }
}
