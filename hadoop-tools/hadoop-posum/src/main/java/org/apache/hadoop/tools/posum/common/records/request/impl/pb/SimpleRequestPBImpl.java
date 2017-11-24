package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.records.request.SimpleRequest;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.SimpleRequestProtoOrBuilder;

public class SimpleRequestPBImpl<T extends Payload> extends SimpleRequest<T> {

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
      builder.setPayload(((PayloadPB) payload).getProtoBytes());
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
    if (payload == null) {
      SimpleRequestProtoOrBuilder p = viaProto ? proto : builder;
      if (p.hasPayload())
        try {
          payload = (T) getType().getPayloadType().getImplClass().newInstance();
          ((PayloadPB) payload).populateFromProtoBytes(p.getPayload());
        } catch (InstantiationException | IllegalAccessException | InvalidProtocolBufferException e) {
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
