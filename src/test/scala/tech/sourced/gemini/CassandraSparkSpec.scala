package tech.sourced.gemini

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CassandraSparkSpec extends FlatSpec
  with Matchers
  with BaseSparkSpec
  with Logging {

  //to start Embedded Cassandra:
  // with SparkTemplate with EmbeddedCassandra
  // useCassandraConfig(Seq(YamlTransformations.Default))
  // override def clearCache(): Unit = CassandraConnector.evictCache()
  // + spark-cassandra-connector/blob/master/spark-cassandra-connector/src/it/resources/cassandra-3.2.yaml.template

  val defaultConf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cassandra.connection.port", "9042")
    .set("spark.cassandra.connection.keep_alive_ms", "5000")
    .set("spark.cassandra.connection.timeout_ms", "30000")
    .set("spark.ui.showConsoleProgress", "false")
    .set("spark.ui.enabled", "false")
    .set("spark.cleaner.ttl", "3600")

  useSparkConf(defaultConf)

  val conn = CassandraConnector(defaultConf)

  def awaitAll(units: TraversableOnce[Future[Any]]): Unit = {
    implicit val ec = scala.concurrent.ExecutionContext.global
    Await.result(Future.sequence(units), Duration.Inf)
  }

  //create schema
  conn.withSessionDo { session =>
    awaitAll(Gemini.applySchema(session, "src/main/resources/schema.cql"))
  }

  "Read from Cassandra" should "return same results as were written" in {
    val gemini = Gemini(sparkSession)

    //TODO(bzz): repo URL list, that will be fetched by Engine
    gemini.hash("src/test/resources/siva")

    conn.withSessionDo { session =>
      val sha1 = Gemini.query("LICENSE", session)
      sha1.head should be("43fa11234bc29cdd498cba1e200edd2dae052fa9") // git hash-object -w LICENSE
    }
  }

}
