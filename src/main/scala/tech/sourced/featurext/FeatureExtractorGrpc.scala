package tech.sourced.featurext

object FeatureExtractorGrpc {
  val METHOD_IDENTIFIERS: _root_.io.grpc.MethodDescriptor[tech.sourced.featurext.IdentifiersRequest, tech.sourced.featurext.FeaturesReply] =
    _root_.io.grpc.MethodDescriptor.newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(_root_.io.grpc.MethodDescriptor.generateFullMethodName("tech.sourced.featurext.FeatureExtractor", "Identifiers"))
      .setRequestMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.IdentifiersRequest))
      .setResponseMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.FeaturesReply))
      .build()
  
  val METHOD_LITERALS: _root_.io.grpc.MethodDescriptor[tech.sourced.featurext.LiteralsRequest, tech.sourced.featurext.FeaturesReply] =
    _root_.io.grpc.MethodDescriptor.newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(_root_.io.grpc.MethodDescriptor.generateFullMethodName("tech.sourced.featurext.FeatureExtractor", "Literals"))
      .setRequestMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.LiteralsRequest))
      .setResponseMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.FeaturesReply))
      .build()
  
  val METHOD_UAST2SEQ: _root_.io.grpc.MethodDescriptor[tech.sourced.featurext.Uast2seqRequest, tech.sourced.featurext.FeaturesReply] =
    _root_.io.grpc.MethodDescriptor.newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(_root_.io.grpc.MethodDescriptor.generateFullMethodName("tech.sourced.featurext.FeatureExtractor", "Uast2seq"))
      .setRequestMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.Uast2seqRequest))
      .setResponseMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.FeaturesReply))
      .build()
  
  val SERVICE: _root_.io.grpc.ServiceDescriptor =
    _root_.io.grpc.ServiceDescriptor.newBuilder("tech.sourced.featurext.FeatureExtractor")
      .setSchemaDescriptor(new _root_.scalapb.grpc.ConcreteProtoFileDescriptorSupplier(tech.sourced.featurext.ServiceProto.javaDescriptor))
      .addMethod(METHOD_IDENTIFIERS)
      .addMethod(METHOD_LITERALS)
      .addMethod(METHOD_UAST2SEQ)
      .build()
  
  trait FeatureExtractor extends _root_.scalapb.grpc.AbstractService {
    override def serviceCompanion = FeatureExtractor
    def identifiers(request: tech.sourced.featurext.IdentifiersRequest): scala.concurrent.Future[tech.sourced.featurext.FeaturesReply]
    def literals(request: tech.sourced.featurext.LiteralsRequest): scala.concurrent.Future[tech.sourced.featurext.FeaturesReply]
    def uast2Seq(request: tech.sourced.featurext.Uast2seqRequest): scala.concurrent.Future[tech.sourced.featurext.FeaturesReply]
  }
  
  object FeatureExtractor extends _root_.scalapb.grpc.ServiceCompanion[FeatureExtractor] {
    implicit def serviceCompanion: _root_.scalapb.grpc.ServiceCompanion[FeatureExtractor] = this
    def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = tech.sourced.featurext.ServiceProto.javaDescriptor.getServices().get(0)
  }
  
  trait FeatureExtractorBlockingClient {
    def serviceCompanion = FeatureExtractor
    def identifiers(request: tech.sourced.featurext.IdentifiersRequest): tech.sourced.featurext.FeaturesReply
    def literals(request: tech.sourced.featurext.LiteralsRequest): tech.sourced.featurext.FeaturesReply
    def uast2Seq(request: tech.sourced.featurext.Uast2seqRequest): tech.sourced.featurext.FeaturesReply
  }
  
  class FeatureExtractorBlockingStub(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT) extends _root_.io.grpc.stub.AbstractStub[FeatureExtractorBlockingStub](channel, options) with FeatureExtractorBlockingClient {
    override def identifiers(request: tech.sourced.featurext.IdentifiersRequest): tech.sourced.featurext.FeaturesReply = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_IDENTIFIERS, options), request)
    }
    
    override def literals(request: tech.sourced.featurext.LiteralsRequest): tech.sourced.featurext.FeaturesReply = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_LITERALS, options), request)
    }
    
    override def uast2Seq(request: tech.sourced.featurext.Uast2seqRequest): tech.sourced.featurext.FeaturesReply = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_UAST2SEQ, options), request)
    }
    
    override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): FeatureExtractorBlockingStub = new FeatureExtractorBlockingStub(channel, options)
  }
  
  class FeatureExtractorStub(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT) extends _root_.io.grpc.stub.AbstractStub[FeatureExtractorStub](channel, options) with FeatureExtractor {
    override def identifiers(request: tech.sourced.featurext.IdentifiersRequest): scala.concurrent.Future[tech.sourced.featurext.FeaturesReply] = {
      scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_IDENTIFIERS, options), request))
    }
    
    override def literals(request: tech.sourced.featurext.LiteralsRequest): scala.concurrent.Future[tech.sourced.featurext.FeaturesReply] = {
      scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_LITERALS, options), request))
    }
    
    override def uast2Seq(request: tech.sourced.featurext.Uast2seqRequest): scala.concurrent.Future[tech.sourced.featurext.FeaturesReply] = {
      scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_UAST2SEQ, options), request))
    }
    
    override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): FeatureExtractorStub = new FeatureExtractorStub(channel, options)
  }
  
  def bindService(serviceImpl: FeatureExtractor, executionContext: scala.concurrent.ExecutionContext): _root_.io.grpc.ServerServiceDefinition =
    _root_.io.grpc.ServerServiceDefinition.builder(SERVICE)
    .addMethod(
      METHOD_IDENTIFIERS,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[tech.sourced.featurext.IdentifiersRequest, tech.sourced.featurext.FeaturesReply] {
        override def invoke(request: tech.sourced.featurext.IdentifiersRequest, observer: _root_.io.grpc.stub.StreamObserver[tech.sourced.featurext.FeaturesReply]): Unit =
          serviceImpl.identifiers(request).onComplete(scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .addMethod(
      METHOD_LITERALS,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[tech.sourced.featurext.LiteralsRequest, tech.sourced.featurext.FeaturesReply] {
        override def invoke(request: tech.sourced.featurext.LiteralsRequest, observer: _root_.io.grpc.stub.StreamObserver[tech.sourced.featurext.FeaturesReply]): Unit =
          serviceImpl.literals(request).onComplete(scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .addMethod(
      METHOD_UAST2SEQ,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[tech.sourced.featurext.Uast2seqRequest, tech.sourced.featurext.FeaturesReply] {
        override def invoke(request: tech.sourced.featurext.Uast2seqRequest, observer: _root_.io.grpc.stub.StreamObserver[tech.sourced.featurext.FeaturesReply]): Unit =
          serviceImpl.uast2Seq(request).onComplete(scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .build()
  
  def blockingStub(channel: _root_.io.grpc.Channel): FeatureExtractorBlockingStub = new FeatureExtractorBlockingStub(channel)
  
  def stub(channel: _root_.io.grpc.Channel): FeatureExtractorStub = new FeatureExtractorStub(channel)
  
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = tech.sourced.featurext.ServiceProto.javaDescriptor.getServices().get(0)
  
}