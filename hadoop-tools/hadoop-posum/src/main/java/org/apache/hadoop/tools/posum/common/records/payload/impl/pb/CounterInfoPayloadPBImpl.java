package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.CounterInfoPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.CounterInfoPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.CounterInfoPayloadProtoOrBuilder;

public class CounterInfoPayloadPBImpl extends CounterInfoPayload implements PayloadPB {
  private CounterInfoPayloadProto proto = CounterInfoPayloadProto.getDefaultInstance();
  private CounterInfoPayloadProto.Builder builder = null;
  private boolean viaProto = false;

  public CounterInfoPayloadPBImpl() {
    builder = CounterInfoPayloadProto.newBuilder();
  }

  public CounterInfoPayloadPBImpl(CounterInfoPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public CounterInfoPayloadProto getProto() {
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
      builder = CounterInfoPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public void setName(String name) {
    maybeInitBuilder();
    builder.setName(name);
  }

  public String getName() {
    CounterInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
    return p.getName();
  }

  public void setTotalCounterValue(long value) {
    maybeInitBuilder();
    builder.setTotal(value);
  }

  public long getTotalCounterValue() {
    CounterInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
    return p.getTotal();
  }

  public void setMapCounterValue(long value) {
    maybeInitBuilder();
    builder.setMap(value);
  }

  public long getMapCounterValue() {
    CounterInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
    return p.getMap();
  }

  public void setReduceCounterValue(long value) {
    maybeInitBuilder();
    builder.setReduce(value);
  }

  public long getReduceCounterValue() {
    CounterInfoPayloadProtoOrBuilder p = viaProto ? proto : builder;
    return p.getReduce();
  }

  public void setValue(long value) {
    maybeInitBuilder();
    builder.setTotal(value);
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = CounterInfoPayloadProto.parseFrom(data);
    viaProto = true;
  }
}