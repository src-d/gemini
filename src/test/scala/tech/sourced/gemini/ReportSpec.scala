package tech.sourced.gemini

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Tag}
import tech.sourced.gemini.util.Logger

// require cassandra itself, not scylla
object Cassandra extends Tag("Cassandra")

@tags.Bblfsh
@tags.FeatureExtractor
@tags.DB
@tags.Spark
class ReportSpec extends FlatSpec
  with Matchers
  with BaseSparkSpec
  with Logging
  with BeforeAndAfterAll {

  val UNIQUES = "test_hashes_uniques"
  val DUPLICATES = "test_hashes_duplicates"

  val expectedDuplicateFiles = List(
    "model_test.go",
    "MAINTAINERS",
    "changes.go",
    "model.go",
    "file.py",
    "cli/borges/version.go",
    "Makefile",
    "doc.go"
  )

  val defaultConf = useDefaultSparkConf()
  var session: Session = _

  val logger = Logger("gemini")

  override def beforeAll(): Unit = {
    super.beforeAll()
    session = CassandraConnector(defaultConf).openSession()
    prepareKeyspace("src/test/resources/siva/unique-files", UNIQUES)
    prepareKeyspace("src/test/resources/siva/duplicate-files", DUPLICATES)
  }

  def prepareKeyspace(sivaPath: String, keyspace: String): Unit = {
    val gemini = Gemini(sparkSession, logger, keyspace)
    gemini.dropSchema(session)
    gemini.applySchema(session)
    val mode = Gemini.fileSimilarityMode
    println(s"Hash ${mode} in ${sivaPath} for keyspace ${keyspace}")
    gemini.hash(sivaPath, mode=mode)
    println("Done")
  }

  override def afterAll(): Unit = {
    Gemini(null, logger, UNIQUES).dropSchema(session)
    Gemini(null, logger, DUPLICATES).dropSchema(session)
    session.close()
    super.afterAll()
  }

  "Report from Cassandra using GROUP BY" should "return duplicate files" taggedAs Cassandra in {
    val report = new Report(session, logger, DUPLICATES, Gemini.tables)

    println("Query")
    val result = report.reportCassandraCondensed()
    println("Done")

    result should have size expectedDuplicateFiles.size
    result foreach (_.count should be(2))
  }

  "Detailed Report from Cassandra using GROUP BY" should "return duplicate files" taggedAs Cassandra in {
    val report = new Report(session, logger, DUPLICATES, Gemini.tables)

    println("Query")
    val detailedReport = report.reportCassandraGroupBy()
    println("Done")

    val duplicatedFileNames = detailedReport map (_.head.path)
    duplicatedFileNames.toSeq should contain theSameElementsAs expectedDuplicateFiles
  }

  "Detailed Report from Database" should "return duplicate files" in {
    val report = new Report(session, logger, DUPLICATES, Gemini.tables)

    println("Query")
    val detailedReport = report.findAllDuplicateItems()
    println("Done")

    val duplicatedFileNames = detailedReport map (_.head.path)
    duplicatedFileNames.toSeq should contain theSameElementsAs expectedDuplicateFiles
  }

  "Report from Database with unique files" should "return no duplicate files" in {
    val report = new Report(session, logger, UNIQUES, Gemini.tables)

    println("Query")
    val result = report.findAllDuplicateItems()
    println("Done")

    result should have size 0
  }
}
