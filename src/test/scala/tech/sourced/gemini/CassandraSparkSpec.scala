package tech.sourced.gemini

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Tag}

class CassandraSparkSpec extends FlatSpec
  with Matchers
  with BaseSparkSpec
  with Logging
  with BeforeAndAfterAll {

  //to start Embedded Cassandra:
  // with SparkTemplate with EmbeddedCassandra
  // useCassandraConfig(Seq(YamlTransformations.Default))
  // override def clearCache(): Unit = CassandraConnector.evictCache()
  // + spark-cassandra-connector/blob/master/spark-cassandra-connector/src/it/resources/cassandra-3.2.yaml.template

  var session: Session = _

  val defaultConf: SparkConf = new SparkConf(true)
    .set("spark.cassandra.connection.host", Gemini.defaultCassandraHost)
    .set("spark.cassandra.connection.port", Gemini.defaultCassandraPort.toString)
    .set("spark.cassandra.connection.keep_alive_ms", "5000")
    .set("spark.cassandra.connection.timeout_ms", "30000")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.ui.enabled", "false")
    .set("spark.cleaner.ttl", "3600")

  useSparkConf(defaultConf)

  val logger = Logger("gemini")

  override def beforeAll(): Unit = {
    super.beforeAll()
    session = CassandraConnector(defaultConf).openSession()
    prepareKeyspace("src/test/resources/siva/unique-files", UNIQUES)
    prepareKeyspace("src/test/resources/siva/duplicate-files", DUPLICATES)
  }

  override def afterAll(): Unit = {
    Gemini(null, logger, UNIQUES).dropSchema(session)
    Gemini(null, logger, DUPLICATES).dropSchema(session)
    super.afterAll()
    session.close()
  }

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

  def prepareKeyspace(sivaPath: String, keyspace: String): Unit = {
    val gemini = Gemini(sparkSession, logger, keyspace)
    gemini.dropSchema(session)
    gemini.applySchema(session)
    println("Hash")
    gemini.hashAndSave(sivaPath)
    println("Done")
  }

  val UNIQUES = "test_hashes_uniques"
  val DUPLICATES = "test_hashes_duplicates"

  object Cassandra extends Tag("Cassandra")

  "Read from Database" should "return same results as written" in {
    val gemini = Gemini(sparkSession, logger, UNIQUES)

    println("Query")
    val sha1 = gemini.query("LICENSE", session)
    println("Done")

    sha1.v should not be empty
    sha1.v.head.sha should be("097f4a292c384e002c5b5ce8e15d746849af7b37") // git hash-object -w LICENSE
    sha1.v.head.repo should be("null/Users/alex/src-d/gemini")
    sha1.v.head.commit should be("4aa29ac236c55ebbfbef149fef7054d25832717f")
  }

  "Query for duplicates in single repository" should "return 2 files" in {
    val gemini = Gemini(sparkSession, logger, DUPLICATES)

    // 2 file in 9279be3cf07fb3cca4fc964b27acea57e0af461b.siva
    val sha1 = Gemini.findDuplicateItemForBlobHash("c4e5bcc8001f80acc238877174130845c5c39aa3", session, DUPLICATES)

    sha1 should not be empty
    sha1.size shouldEqual 2
  }

  "Report from Cassandra using GROUP BY" should "return duplicate files" taggedAs Cassandra in {
    val gemini = Gemini(sparkSession, logger, DUPLICATES)

    println("Query")
    val report = gemini.reportCassandraCondensed(session).v
    println("Done")

    report should have size expectedDuplicateFiles.size
    report foreach (_.count should be(2))
  }

  "Detailed Report from Cassandra using GROUP BY" should "return duplicate files" taggedAs Cassandra in {
    val gemini = Gemini(sparkSession, logger, DUPLICATES)

    println("Query")
    val detailedReport = gemini.reportCassandraGroupBy(session).v
    println("Done")

    val duplicatedFileNames = detailedReport map (_.head.path)
    duplicatedFileNames.toSeq should contain theSameElementsAs expectedDuplicateFiles
  }

  "Detailed Report from Database" should "return duplicate files" in {
    val gemini = Gemini(sparkSession, logger, DUPLICATES)

    println("Query")
    val detailedReport = gemini.report(session).v
    println("Done")

    val duplicatedFileNames = detailedReport map (_.head.path)
    duplicatedFileNames.toSeq should contain theSameElementsAs expectedDuplicateFiles
  }

  "Report from Database with unique files" should "return no duplicate files" in {
    val gemini = Gemini(sparkSession, logger, UNIQUES)

    println("Query")
    val report = gemini.report(session)
    println("Done")

    report should have size 0
  }

  "Hash with limit" should "collect files only from limit repos" in {
    val gemini = Gemini(sparkSession)
    val repos = gemini.hash("src/test/resources/siva", 1).select(Gemini.meta.repo).distinct().count()
    repos should be(1)
  }

  //TODO(bzz): add test \w repo URL list, that will be fetched by Engine
}
