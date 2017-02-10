package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos.SimplePropertyPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimplePropertyPayloadProtoOrBuilder;

import java.io.IOException;

@JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class SimplePropertyPayloadPBImpl extends SimplePropertyPayload implements PayloadPB {
  private SimplePropertyPayloadProto proto = SimplePropertyPayloadProto.getDefaultInstance();
  private SimplePropertyPayloadProto.Builder builder = null;
  private boolean viaProto = false;

  public SimplePropertyPayloadPBImpl() {
    builder = SimplePropertyPayloadProto.newBuilder();
  }

  public SimplePropertyPayloadPBImpl(SimplePropertyPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @JsonIgnore
  @org.codehaus.jackson.annotate.JsonIgnore
  public SimplePropertyPayloadProto getProto() {
    mergeLocalToProto();
    proto = viaProto ? proto : builder.build();
    viaProto = true;
    return proto;
  }

  @Override
  public int hashCode() {
    return getProto().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null)
      return false;
    if (other.getClass().isAssignableFrom(this.getClass())) {
      return this.getProto().equals(this.getClass().cast(other).getProto());
    }
    return false;
  }

  @Override
  public String toString() {
    return TextFormat.shortDebugString(getProto());
  }

  private void mergeLocalToBuilder() {

  }

  private void mergeLocalToProto() {
    if (viaProto)
      maybeInitBuilder();
    mergeLocalToBuilder();
    proto = builder.build();
    viaProto = true;
  }

  private void maybeInitBuilder() {
    if (viaProto || builder == null) {
      builder = SimplePropertyPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public PropertyType getType() {
    SimplePropertyPayloadProtoOrBuilder p = viaProto ? proto : builder;
    return PropertyType.valueOf(p.getType().name().substring("PROP_".length()));
  }

  @Override
  public void setType(SimplePropertyPayload.PropertyType type) {
    maybeInitBuilder();
    if (type == null) {
      builder.clearType();
      return;
    }
    builder.setType(SimplePropertyPayloadProto.PropertyTypeProto.valueOf("PROP_" + type.name()));
  }

  @Override
  public Object getValue() {
    SimplePropertyPayloadProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasValue())
      return null;
    try {
      return getType().read(p.getValue());
    } catch (IOException e) {
      throw new PosumException("Error reading property value ", e);
    }
  }

  @Override
  public void setValue(Object value) {
    maybeInitBuilder();
    if (value == null) {
      builder.clearValue();
      return;
    }
    builder.setValue(value.toString());
  }

  @Override
  public String getName() {
    SimplePropertyPayloadProtoOrBuilder p = viaProto ? proto : builder;
    return p.getName();
  }

  @Override
  public void setName(String name) {
    maybeInitBuilder();
    if (name == null) {
      builder.clearName();
      return;
    }
    builder.setName(name);
  }

  @Override
  @JsonIgnore
  @org.codehaus.jackson.annotate.JsonIgnore
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = SimplePropertyPayloadProto.parseFrom(data);
    viaProto = true;
  }


}