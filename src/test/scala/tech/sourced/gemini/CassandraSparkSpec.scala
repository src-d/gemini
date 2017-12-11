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
    awaitAll(Gemini.applySchema(session, "src/main/resources/schema.cql"))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    session.close()
  }

  def awaitAll(units: TraversableOnce[Future[Any]]): Unit = {
    implicit val ec = scala.concurrent.ExecutionContext.global
    Await.result(Future.sequence(units), Duration.Inf)
  }

  "Read from Cassandra" should "return same results as were written" in {
    val gemini = Gemini(sparkSession)

    //TODO(bzz): repo URL list, that will be fetched by Engine
    gemini.hash("src/test/resources/siva")

    val sha1 = Gemini.query("LICENSE", session)
    sha1.head.sha should be("097f4a292c384e002c5b5ce8e15d746849af7b37") // git hash-object -w LICENSE
  }

}
