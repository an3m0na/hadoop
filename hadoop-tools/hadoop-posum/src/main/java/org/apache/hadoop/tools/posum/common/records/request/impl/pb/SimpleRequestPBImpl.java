package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProtoOrBuilder;

public abstract class SimpleRequestPBImpl<T> extends SimpleRequest<T> {

  public SimpleRequestPBImpl() {
    this.proto = SimpleRequestProto.getDefaultInstance();
  }

  public SimpleRequestPBImpl(SimpleRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  private SimpleRequestProto proto;
  private SimpleRequestProto.Builder builder;
  private boolean viaProto = false;

  private T payload;

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
    maybeInitBuilder();
    if (this.payload != null)
      builder.setPayload(payloadToBytes(payload));
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
      builder = SimpleRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public SimpleRequestProto getProto() {
    if (!viaProto)
      mergeLocalToProto();
    return proto;
  }

  public abstract ByteString payloadToBytes(T payload);

  public abstract T bytesToPayload(ByteString data) throws InvalidProtocolBufferException;

  @Override
  public Type getType() {
    SimpleRequestProtoOrBuilder p = viaProto ? proto : builder;
    return Type.fromProto(p.getType());
  }

  @Override
  public void setType(Type type) {
    maybeInitBuilder();
    if (type != null)
      builder.setType(type.toProto());
  }

  @Override
  public T getPayload() {
    if (this.payload == null) {
      SimpleRequestProtoOrBuilder p = viaProto ? proto : builder;
      if (p.hasPayload())
        try {
          this.payload = bytesToPayload(p.getPayload());
        } catch (InvalidProtocolBufferException e) {
          throw new PosumException("Could not read message payload", e);
        }
    }
    return payload;
  }

  @Override
  public void setPayload(T payload) {
    maybeInitBuilder();
    if(payload == null){
      builder.clearPayload();
    }
    this.payload = payload;
  }
}
