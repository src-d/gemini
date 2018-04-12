package tech.sourced.featurext.generated.service

object FeatureExtractorGrpc {
  val METHOD_IDENTIFIERS: _root_.io.grpc.MethodDescriptor[tech.sourced.featurext.generated.service.IdentifiersRequest, tech.sourced.featurext.generated.service.FeaturesReply] =
    _root_.io.grpc.MethodDescriptor.newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(_root_.io.grpc.MethodDescriptor.generateFullMethodName("tech.sourced.featurext.generated.FeatureExtractor", "Identifiers"))
      .setRequestMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.generated.service.IdentifiersRequest))
      .setResponseMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.generated.service.FeaturesReply))
      .build()
  
  val METHOD_LITERALS: _root_.io.grpc.MethodDescriptor[tech.sourced.featurext.generated.service.LiteralsRequest, tech.sourced.featurext.generated.service.FeaturesReply] =
    _root_.io.grpc.MethodDescriptor.newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(_root_.io.grpc.MethodDescriptor.generateFullMethodName("tech.sourced.featurext.generated.FeatureExtractor", "Literals"))
      .setRequestMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.generated.service.LiteralsRequest))
      .setResponseMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.generated.service.FeaturesReply))
      .build()
  
  val METHOD_UAST2SEQ: _root_.io.grpc.MethodDescriptor[tech.sourced.featurext.generated.service.Uast2seqRequest, tech.sourced.featurext.generated.service.FeaturesReply] =
    _root_.io.grpc.MethodDescriptor.newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(_root_.io.grpc.MethodDescriptor.generateFullMethodName("tech.sourced.featurext.generated.FeatureExtractor", "Uast2seq"))
      .setRequestMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.generated.service.Uast2seqRequest))
      .setResponseMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.generated.service.FeaturesReply))
      .build()
  
  val METHOD_GRAPHLET: _root_.io.grpc.MethodDescriptor[tech.sourced.featurext.generated.service.GraphletRequest, tech.sourced.featurext.generated.service.FeaturesReply] =
    _root_.io.grpc.MethodDescriptor.newBuilder()
      .setType(_root_.io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(_root_.io.grpc.MethodDescriptor.generateFullMethodName("tech.sourced.featurext.generated.FeatureExtractor", "Graphlet"))
      .setRequestMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.generated.service.GraphletRequest))
      .setResponseMarshaller(new scalapb.grpc.Marshaller(tech.sourced.featurext.generated.service.FeaturesReply))
      .build()
  
  val SERVICE: _root_.io.grpc.ServiceDescriptor =
    _root_.io.grpc.ServiceDescriptor.newBuilder("tech.sourced.featurext.generated.FeatureExtractor")
      .setSchemaDescriptor(new _root_.scalapb.grpc.ConcreteProtoFileDescriptorSupplier(tech.sourced.featurext.generated.service.ServiceProto.javaDescriptor))
      .addMethod(METHOD_IDENTIFIERS)
      .addMethod(METHOD_LITERALS)
      .addMethod(METHOD_UAST2SEQ)
      .addMethod(METHOD_GRAPHLET)
      .build()
  
  trait FeatureExtractor extends _root_.scalapb.grpc.AbstractService {
    override def serviceCompanion = FeatureExtractor
    def identifiers(request: tech.sourced.featurext.generated.service.IdentifiersRequest): scala.concurrent.Future[tech.sourced.featurext.generated.service.FeaturesReply]
    def literals(request: tech.sourced.featurext.generated.service.LiteralsRequest): scala.concurrent.Future[tech.sourced.featurext.generated.service.FeaturesReply]
    def uast2Seq(request: tech.sourced.featurext.generated.service.Uast2seqRequest): scala.concurrent.Future[tech.sourced.featurext.generated.service.FeaturesReply]
    def graphlet(request: tech.sourced.featurext.generated.service.GraphletRequest): scala.concurrent.Future[tech.sourced.featurext.generated.service.FeaturesReply]
  }
  
  object FeatureExtractor extends _root_.scalapb.grpc.ServiceCompanion[FeatureExtractor] {
    implicit def serviceCompanion: _root_.scalapb.grpc.ServiceCompanion[FeatureExtractor] = this
    def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = tech.sourced.featurext.generated.service.ServiceProto.javaDescriptor.getServices().get(0)
  }
  
  trait FeatureExtractorBlockingClient {
    def serviceCompanion = FeatureExtractor
    def identifiers(request: tech.sourced.featurext.generated.service.IdentifiersRequest): tech.sourced.featurext.generated.service.FeaturesReply
    def literals(request: tech.sourced.featurext.generated.service.LiteralsRequest): tech.sourced.featurext.generated.service.FeaturesReply
    def uast2Seq(request: tech.sourced.featurext.generated.service.Uast2seqRequest): tech.sourced.featurext.generated.service.FeaturesReply
    def graphlet(request: tech.sourced.featurext.generated.service.GraphletRequest): tech.sourced.featurext.generated.service.FeaturesReply
  }
  
  class FeatureExtractorBlockingStub(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT) extends _root_.io.grpc.stub.AbstractStub[FeatureExtractorBlockingStub](channel, options) with FeatureExtractorBlockingClient {
    override def identifiers(request: tech.sourced.featurext.generated.service.IdentifiersRequest): tech.sourced.featurext.generated.service.FeaturesReply = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_IDENTIFIERS, options), request)
    }
    
    override def literals(request: tech.sourced.featurext.generated.service.LiteralsRequest): tech.sourced.featurext.generated.service.FeaturesReply = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_LITERALS, options), request)
    }
    
    override def uast2Seq(request: tech.sourced.featurext.generated.service.Uast2seqRequest): tech.sourced.featurext.generated.service.FeaturesReply = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_UAST2SEQ, options), request)
    }
    
    override def graphlet(request: tech.sourced.featurext.generated.service.GraphletRequest): tech.sourced.featurext.generated.service.FeaturesReply = {
      _root_.io.grpc.stub.ClientCalls.blockingUnaryCall(channel.newCall(METHOD_GRAPHLET, options), request)
    }
    
    override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): FeatureExtractorBlockingStub = new FeatureExtractorBlockingStub(channel, options)
  }
  
  class FeatureExtractorStub(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions = _root_.io.grpc.CallOptions.DEFAULT) extends _root_.io.grpc.stub.AbstractStub[FeatureExtractorStub](channel, options) with FeatureExtractor {
    override def identifiers(request: tech.sourced.featurext.generated.service.IdentifiersRequest): scala.concurrent.Future[tech.sourced.featurext.generated.service.FeaturesReply] = {
      scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_IDENTIFIERS, options), request))
    }
    
    override def literals(request: tech.sourced.featurext.generated.service.LiteralsRequest): scala.concurrent.Future[tech.sourced.featurext.generated.service.FeaturesReply] = {
      scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_LITERALS, options), request))
    }
    
    override def uast2Seq(request: tech.sourced.featurext.generated.service.Uast2seqRequest): scala.concurrent.Future[tech.sourced.featurext.generated.service.FeaturesReply] = {
      scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_UAST2SEQ, options), request))
    }
    
    override def graphlet(request: tech.sourced.featurext.generated.service.GraphletRequest): scala.concurrent.Future[tech.sourced.featurext.generated.service.FeaturesReply] = {
      scalapb.grpc.Grpc.guavaFuture2ScalaFuture(_root_.io.grpc.stub.ClientCalls.futureUnaryCall(channel.newCall(METHOD_GRAPHLET, options), request))
    }
    
    override def build(channel: _root_.io.grpc.Channel, options: _root_.io.grpc.CallOptions): FeatureExtractorStub = new FeatureExtractorStub(channel, options)
  }
  
  def bindService(serviceImpl: FeatureExtractor, executionContext: scala.concurrent.ExecutionContext): _root_.io.grpc.ServerServiceDefinition =
    _root_.io.grpc.ServerServiceDefinition.builder(SERVICE)
    .addMethod(
      METHOD_IDENTIFIERS,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[tech.sourced.featurext.generated.service.IdentifiersRequest, tech.sourced.featurext.generated.service.FeaturesReply] {
        override def invoke(request: tech.sourced.featurext.generated.service.IdentifiersRequest, observer: _root_.io.grpc.stub.StreamObserver[tech.sourced.featurext.generated.service.FeaturesReply]): Unit =
          serviceImpl.identifiers(request).onComplete(scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .addMethod(
      METHOD_LITERALS,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[tech.sourced.featurext.generated.service.LiteralsRequest, tech.sourced.featurext.generated.service.FeaturesReply] {
        override def invoke(request: tech.sourced.featurext.generated.service.LiteralsRequest, observer: _root_.io.grpc.stub.StreamObserver[tech.sourced.featurext.generated.service.FeaturesReply]): Unit =
          serviceImpl.literals(request).onComplete(scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .addMethod(
      METHOD_UAST2SEQ,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[tech.sourced.featurext.generated.service.Uast2seqRequest, tech.sourced.featurext.generated.service.FeaturesReply] {
        override def invoke(request: tech.sourced.featurext.generated.service.Uast2seqRequest, observer: _root_.io.grpc.stub.StreamObserver[tech.sourced.featurext.generated.service.FeaturesReply]): Unit =
          serviceImpl.uast2Seq(request).onComplete(scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .addMethod(
      METHOD_GRAPHLET,
      _root_.io.grpc.stub.ServerCalls.asyncUnaryCall(new _root_.io.grpc.stub.ServerCalls.UnaryMethod[tech.sourced.featurext.generated.service.GraphletRequest, tech.sourced.featurext.generated.service.FeaturesReply] {
        override def invoke(request: tech.sourced.featurext.generated.service.GraphletRequest, observer: _root_.io.grpc.stub.StreamObserver[tech.sourced.featurext.generated.service.FeaturesReply]): Unit =
          serviceImpl.graphlet(request).onComplete(scalapb.grpc.Grpc.completeObserver(observer))(
            executionContext)
      }))
    .build()
  
  def blockingStub(channel: _root_.io.grpc.Channel): FeatureExtractorBlockingStub = new FeatureExtractorBlockingStub(channel)
  
  def stub(channel: _root_.io.grpc.Channel): FeatureExtractorStub = new FeatureExtractorStub(channel)
  
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.ServiceDescriptor = tech.sourced.featurext.generated.service.ServiceProto.javaDescriptor.getServices().get(0)
  
}