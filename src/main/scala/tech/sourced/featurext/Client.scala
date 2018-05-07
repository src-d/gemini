package tech.sourced.featurext

import gopkg.in.bblfsh.sdk.v1.uast.generated.Node
import io.grpc.ManagedChannelBuilder
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import tech.sourced.featurext.generated.service.FeatureExtractorGrpc.FeatureExtractor
import tech.sourced.featurext.generated.service._
import org.slf4j.{Logger => Slf4jLogger}

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, SECONDS}
import scala.util.control.NonFatal

object FEClient {
  val idWeight = 194
  val literalsWeight = 264
  val graphletWeight = 548

  def extract(uast: Node, client: FeatureExtractor, log: Slf4jLogger): Iterable[Feature] = {
    val idRequest = IdentifiersRequest(uast=Some(uast), docfreqThreshold=5, weight = idWeight, splitStem = true)
    val litRequest = LiteralsRequest(uast=Some(uast), docfreqThreshold=5, weight = literalsWeight)
    val graphletRequest = GraphletRequest(uast=Some(uast), docfreqThreshold=5, weight = graphletWeight)
    client.identifiers(idRequest)

    try {
      val features = for {
        idResponse <- client.identifiers(idRequest)
        litResponse <- client.literals(litRequest)
        graphletResponse <- client.graphlet(graphletRequest)
      } yield idResponse.features ++ litResponse.features ++ graphletResponse.features

      Await.result(features, Duration(30, SECONDS))
    } catch {
      case NonFatal(e) => {
        log.error(s"feature extractor error: ${e.toString}")
        Iterable[Feature]()
      }
    }
  }

}

object SparkFEClient extends Logging {
  case class Config(host: String, port: Int)

  /** Key used for the option to specify the host of the feature extractor grpc service. */
  val hostKey = "spark.tech.sourced.featurext.grpc.host"

  /** Key used for the option to specify the port of the feature extractor grpc service. */
  val portKey = "spark.tech.sourced.featurext.grpc.port"

  /** Default service host. */
  val defaultHost = "127.0.0.1"

  /** Default service port. */
  val defaultPort = 9001

  private var config: Config = _
  private var client: FeatureExtractor = _

  /**
    * Returns the configuration for feature extractor.
    *
    * @param session Spark session
    * @return featurext configuration
    */
  def getConfig(session: SparkSession): Config = {
    if (config == null) {
      val host = session.conf.get(hostKey, SparkFEClient.defaultHost)
      val port = session.conf.get(portKey, SparkFEClient.defaultPort.toString).toInt
      config = Config(host, port)
    }

    config
  }

  private def getClient(config: Config): FeatureExtractor = synchronized {
    if (client == null) {
      val channel = ManagedChannelBuilder.forAddress(config.host, config.port).usePlaintext(true).build()
      client = FeatureExtractorGrpc.stub(channel)
    }

    client
  }

  def extract(uast: Node, config: Config): Iterable[Feature] = {
    val client = getClient(config)
    FEClient.extract(uast, client, log)
  }

}
