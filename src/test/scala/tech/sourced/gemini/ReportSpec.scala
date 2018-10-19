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
  val DUP_FUNCS = "test_hashes_duplicates_funcs"

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
    // TODO we might want to save results of hash as fixtures to speedup tests and
    // remove hard coupling of hash and report (hash tests should fail when it's broken, not report)
    prepareKeyspace("src/test/resources/siva/unique-files", UNIQUES)
    prepareKeyspace("src/test/resources/siva/duplicate-files", DUPLICATES)
    prepareKeyspace("src/test/resources/siva/duplicate-funcs", DUP_FUNCS, Gemini.funcSimilarityMode)
  }

  def prepareKeyspace(sivaPath: String, keyspace: String, mode: String = Gemini.fileSimilarityMode): Unit = {
    val gemini = Gemini(sparkSession, logger, keyspace)
    gemini.dropSchema(session)
    gemini.applySchema(session)
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

  "Detailed Report from Database" should "return similar files" in {
    val report = new Report(session, logger, DUPLICATES, Gemini.tables)

    println("Query")
    val similarGroups = report.findSimilarItems("/tmp/report-files-test", Gemini.fileSimilarityMode)
    println("Done")

    similarGroups should have size 6

    val files = similarGroups.head.map(_.toString)
    files.toSeq should contain theSameElementsAs Seq(
      "https://github.com/src-d/borges/blob/e784f9d5f59d5c081c5f8f71b6c517918b899df0/consumer_test.go",
      "https://github.com/erizocosmico/borges/blob/b1fcd3bf0ba810c05cb418babc09cc7f7783cc03/consumer_test.go"
    )
  }

  "Detailed Report from Database" should "return similar funcs" in {
    val report = new Report(session, logger, DUP_FUNCS, Gemini.tables)

    println("Query")
    val similarGroups = report.findSimilarItems("/tmp/report-funcs-test", Gemini.funcSimilarityMode)
    println("Done")

    similarGroups should have size 1

    val funcs = similarGroups.head.map(_.toString)
    // compare strings instead of objects (I couldn't find func like theSameElementsAs but that checks only values)
    funcs.toSeq should contain theSameElementsAs Seq(
      SimilarFunc(
        RepoFile(
          "null/Users/smacker/tmp/wrap/func-level-repo/",
          "c59310465d5644d3b8c4dbdef765691c0adac52b",
          "func_mod.go",
          "df02c3e5db6d316243a4f16b1733f2945a4eaa1d"
        ),
        "BDuplicate",
        "7"
      ),
      SimilarFunc(
        RepoFile(
          "null/Users/smacker/tmp/wrap/func-level-repo/",
          "c59310465d5644d3b8c4dbdef765691c0adac52b",
          "funcs.go",
          "cf4cf63cf8da4243dd04bd7e1c09ecfda2567d7e"
        ),
        "B",
        "9"
      )
    ).map(_.toString)
  }
}
