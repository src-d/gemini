package tech.sourced.gemini

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{EmbeddedCassandra, SparkTemplate, YamlTransformations}
import org.apache.spark.internal.Logging
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class CassandraSparkITSpec extends FlatSpec
  with Matchers
  with EmbeddedCassandra with SparkTemplate
  with Logging {

  //start empty Embedded Cassandra
  useCassandraConfig(Seq(YamlTransformations.Default))
  useSparkConf(defaultConf)

  val conn = CassandraConnector(SparkTemplate.defaultConf)

  def awaitAll(units: Future[Any]*): Unit = {
    implicit val ec = scala.concurrent.ExecutionContext.global
    Await.result(Future.sequence(units), Duration.Inf)
  }

  //create schema
  conn.withSessionDo { session =>
    awaitAll(Gemini.applySchema(session, "src/main/resources/schema.cql"):_*)
  }

  "Read from Cassandra" should "return same results as were written" in {
    val gemini = Gemini(sparkSession)

    //TODO(bzz): repo URL list, that will be fetched by Engine
    gemini.hash("src/test/resourced/siva")

    conn.withSessionDo { session =>
    //read some hashes by one, using Query check that they present in Cassandra
    //for each file:
      val sha1 = Gemini.query("LICENSE", session)
      sha1.head should be("43fa11234bc29cdd498cba1e200edd2dae052fa9") // git hash-object -w README.md
    }

  }

  override def clearCache(): Unit = CassandraConnector.evictCache()

}
