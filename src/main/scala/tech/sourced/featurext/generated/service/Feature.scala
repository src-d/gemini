// Generated by the Scala Plugin for the Protocol Buffer Compiler.
// Do not edit!
//
// Protofile syntax: PROTO3

package tech.sourced.featurext.generated.service

@SerialVersionUID(0L)
final case class Feature(
    name: _root_.scala.Predef.String = "",
    weight: _root_.scala.Float = 0.0f
    ) extends scalapb.GeneratedMessage with scalapb.Message[Feature] with scalapb.lenses.Updatable[Feature] {
    @transient
    private[this] var __serializedSizeCachedValue: _root_.scala.Int = 0
    private[this] def __computeSerializedValue(): _root_.scala.Int = {
      var __size = 0
      if (name != "") { __size += _root_.com.google.protobuf.CodedOutputStream.computeStringSize(1, name) }
      if (weight != 0.0f) { __size += _root_.com.google.protobuf.CodedOutputStream.computeFloatSize(2, weight) }
      __size
    }
    final override def serializedSize: _root_.scala.Int = {
      var read = __serializedSizeCachedValue
      if (read == 0) {
        read = __computeSerializedValue()
        __serializedSizeCachedValue = read
      }
      read
    }
    def writeTo(`_output__`: _root_.com.google.protobuf.CodedOutputStream): Unit = {
      {
        val __v = name
        if (__v != "") {
          _output__.writeString(1, __v)
        }
      };
      {
        val __v = weight
        if (__v != 0.0f) {
          _output__.writeFloat(2, __v)
        }
      };
    }
    def mergeFrom(`_input__`: _root_.com.google.protobuf.CodedInputStream): tech.sourced.featurext.generated.service.Feature = {
      var __name = this.name
      var __weight = this.weight
      var _done__ = false
      while (!_done__) {
        val _tag__ = _input__.readTag()
        _tag__ match {
          case 0 => _done__ = true
          case 10 =>
            __name = _input__.readString()
          case 21 =>
            __weight = _input__.readFloat()
          case tag => _input__.skipField(tag)
        }
      }
      tech.sourced.featurext.generated.service.Feature(
          name = __name,
          weight = __weight
      )
    }
    def withName(__v: _root_.scala.Predef.String): Feature = copy(name = __v)
    def withWeight(__v: _root_.scala.Float): Feature = copy(weight = __v)
    def getFieldByNumber(__fieldNumber: _root_.scala.Int): scala.Any = {
      (__fieldNumber: @_root_.scala.unchecked) match {
        case 1 => {
          val __t = name
          if (__t != "") __t else null
        }
        case 2 => {
          val __t = weight
          if (__t != 0.0f) __t else null
        }
      }
    }
    def getField(__field: _root_.scalapb.descriptors.FieldDescriptor): _root_.scalapb.descriptors.PValue = {
      require(__field.containingMessage eq companion.scalaDescriptor)
      (__field.number: @_root_.scala.unchecked) match {
        case 1 => _root_.scalapb.descriptors.PString(name)
        case 2 => _root_.scalapb.descriptors.PFloat(weight)
      }
    }
    def toProtoString: _root_.scala.Predef.String = _root_.scalapb.TextFormat.printToUnicodeString(this)
    def companion = tech.sourced.featurext.generated.service.Feature
}

object Feature extends scalapb.GeneratedMessageCompanion[tech.sourced.featurext.generated.service.Feature] {
  implicit def messageCompanion: scalapb.GeneratedMessageCompanion[tech.sourced.featurext.generated.service.Feature] = this
  def fromFieldsMap(__fieldsMap: scala.collection.immutable.Map[_root_.com.google.protobuf.Descriptors.FieldDescriptor, scala.Any]): tech.sourced.featurext.generated.service.Feature = {
    require(__fieldsMap.keys.forall(_.getContainingType() == javaDescriptor), "FieldDescriptor does not match message type.")
    val __fields = javaDescriptor.getFields
    tech.sourced.featurext.generated.service.Feature(
      __fieldsMap.getOrElse(__fields.get(0), "").asInstanceOf[_root_.scala.Predef.String],
      __fieldsMap.getOrElse(__fields.get(1), 0.0f).asInstanceOf[_root_.scala.Float]
    )
  }
  implicit def messageReads: _root_.scalapb.descriptors.Reads[tech.sourced.featurext.generated.service.Feature] = _root_.scalapb.descriptors.Reads{
    case _root_.scalapb.descriptors.PMessage(__fieldsMap) =>
      require(__fieldsMap.keys.forall(_.containingMessage == scalaDescriptor), "FieldDescriptor does not match message type.")
      tech.sourced.featurext.generated.service.Feature(
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(1).get).map(_.as[_root_.scala.Predef.String]).getOrElse(""),
        __fieldsMap.get(scalaDescriptor.findFieldByNumber(2).get).map(_.as[_root_.scala.Float]).getOrElse(0.0f)
      )
    case _ => throw new RuntimeException("Expected PMessage")
  }
  def javaDescriptor: _root_.com.google.protobuf.Descriptors.Descriptor = ServiceProto.javaDescriptor.getMessageTypes.get(9)
  def scalaDescriptor: _root_.scalapb.descriptors.Descriptor = ServiceProto.scalaDescriptor.messages(9)
  def messageCompanionForFieldNumber(__number: _root_.scala.Int): _root_.scalapb.GeneratedMessageCompanion[_] = throw new MatchError(__number)
  lazy val nestedMessagesCompanions: Seq[_root_.scalapb.GeneratedMessageCompanion[_]] = Seq.empty
  def enumCompanionForFieldNumber(__fieldNumber: _root_.scala.Int): _root_.scalapb.GeneratedEnumCompanion[_] = throw new MatchError(__fieldNumber)
  lazy val defaultInstance = tech.sourced.featurext.generated.service.Feature(
  )
  implicit class FeatureLens[UpperPB](_l: _root_.scalapb.lenses.Lens[UpperPB, tech.sourced.featurext.generated.service.Feature]) extends _root_.scalapb.lenses.ObjectLens[UpperPB, tech.sourced.featurext.generated.service.Feature](_l) {
    def name: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Predef.String] = field(_.name)((c_, f_) => c_.copy(name = f_))
    def weight: _root_.scalapb.lenses.Lens[UpperPB, _root_.scala.Float] = field(_.weight)((c_, f_) => c_.copy(weight = f_))
  }
  final val NAME_FIELD_NUMBER = 1
  final val WEIGHT_FIELD_NUMBER = 2
}
