package tech.sourced.featurext;

import io.grpc.ManagedChannelBuilder
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import tech.sourced.featurext.generated._
import gopkg.in.bblfsh.sdk.v1.uast.Node
import java.nio.file.{Files, Paths}

@tags.FEIntegration
class ClientSpec extends FlatSpec
  with Matchers
  with BeforeAndAfterAll {

  val serverHost = "localhost"
  val serverPort = 9001
  val fixturePath = "src/test/resources/protomsgs/server.py.proto"

  var blockingStub: FeatureExtractorGrpc.FeatureExtractorBlockingStub = _
  var uast: Node = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val byteArray: Array[Byte] = Files.readAllBytes(Paths.get(fixturePath))
    uast = Node.parseFrom(byteArray)

    val channel = ManagedChannelBuilder.forAddress(serverHost, serverPort).usePlaintext(true).build()
    blockingStub = FeatureExtractorGrpc.blockingStub(channel)
  }

  "identifiers call" should "return correct response" in {
    val request = IdentifiersRequest(docfreqThreshold=5, uast=Some(uast))
    val reply = blockingStub.identifiers(request)

    // check correct shape of response
    reply.features.size should be(49)
    reply.features(0).name should be("i.sys")
    reply.features(0).weight should be(1)
  }

  "literals call" should "return correct response" in {
    val request = LiteralsRequest(docfreqThreshold=5, uast=Some(uast))
    val reply = blockingStub.literals(request)

    // check correct shape of response
    reply.features.size should be(16)
    reply.features(0).name should be("l.3b286224b098296c")
    reply.features(0).weight should be(1)
  }

  "uast2seq call" should "return correct response" in {
    val request = Uast2seqRequest(docfreqThreshold=5, uast=Some(uast))
    val reply = blockingStub.uast2Seq(request)

    // check correct shape of response
    reply.features.size should be(207)
    reply.features(0).name should be("s.alias>NoopLine>PreviousNoops>Import>Str")
    reply.features(0).weight should be(1)
  }
}
