package tech.sourced.gemini

import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

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

  val defaultConf = new SparkConf(true)
    .set("spark.cassandra.connection.host", Gemini.defaultCassandraHost)
    .set("spark.cassandra.connection.port", Gemini.defaultCassandraPort)
    .set("spark.cassandra.connection.keep_alive_ms", "5000")
    .set("spark.cassandra.connection.timeout_ms", "30000")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.ui.enabled", "false")
    .set("spark.cleaner.ttl", "3600")

  useSparkConf(defaultConf)

  override def beforeAll(): Unit = {
    super.beforeAll()
    session = CassandraConnector(defaultConf).openSession()
    prepareKeyspace("src/test/resources/siva/unique-files", UNIQUES)
    prepareKeyspace("src/test/resources/siva/duplicate-files", DUPLICATES)
  }

  override def afterAll(): Unit = {
    Gemini(null, UNIQUES).dropSchema(session)
    Gemini(null, DUPLICATES).dropSchema(session)
    super.afterAll()
    session.close()
  }

  def awaitAll(units: TraversableOnce[Future[Any]]): Unit = {
    implicit val ec = scala.concurrent.ExecutionContext.global
    Await.result(Future.sequence(units), Duration.Inf)
  }

  val shouldBeDuplicateFileNames = List(
    "model_test.go",
    "MAINTAINERS",
    "changes.go",
    "model.go",
    "cli/borges/version.go",
    "Makefile",
    "doc.go"
  )

  def prepareKeyspace(sivaPath: String, keyspace: String): Unit = {
    val gemini = Gemini(sparkSession, keyspace)
    gemini.dropSchema(session)
    gemini.applySchema(session)
    println("Hash")
    gemini.hashAndSave(sivaPath)
    println("Done")
  }

  val UNIQUES = "test_hashes_uniques"
  val DUPLICATES = "test_hashes_duplicates"

  "Read from Cassandra" should "return same results as written" in {
    val gemini = Gemini(sparkSession, UNIQUES)

    println("Query")
    val sha1 = gemini.query("LICENSE", session)
    println("Done")

    sha1.v.head.sha should be("097f4a292c384e002c5b5ce8e15d746849af7b37") // git hash-object -w LICENSE
  }

  "Report from Cassandra using GROUP BY" should "return duplicate files" in {
    val gemini = Gemini(null, DUPLICATES)

    println("Query")
    val report = gemini.reportCassandraCondensed(session).v
    println("Done")

    report should have size (shouldBeDuplicateFileNames.size)
    report foreach (_.count should be(2))
  }

  "Detailed Report from Cassandra using GROUP BY" should "return duplicate files" in {
    val gemini = Gemini(null, DUPLICATES)

    println("Query")
    val detailedReport = gemini.reportCassandraGroupBy(session).v
    println("Done")

    val duplicatedFileNames = detailedReport map (_.head.file)
    duplicatedFileNames.toSeq should contain theSameElementsAs (shouldBeDuplicateFileNames)
  }

  "Detailed Report from Database" should "return duplicate files" in {
    val gemini = Gemini(null, DUPLICATES)

    println("Query")
    val detailedReport = gemini.report(session).v
    println("Done")

    val duplicatedFileNames = detailedReport map (_.head.file)
    duplicatedFileNames.toSeq should contain theSameElementsAs (shouldBeDuplicateFileNames)
  }

  "Report from Cassandra with unique files" should "return no duplicate files" in {
    val gemini = Gemini(null, UNIQUES)

    println("Query")
    val report = gemini.report(session)
    println("Done")

    report should have size (0)
  }

  //TODO(bzz): add test \w repo URL list, that will be fetched by Engine
}
