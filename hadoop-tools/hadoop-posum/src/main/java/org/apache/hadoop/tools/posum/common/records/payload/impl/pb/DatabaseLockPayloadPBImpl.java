package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DatabaseReferencePBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.DatabaseLockPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseLockPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseLockPayloadProtoOrBuilder;

public class DatabaseLockPayloadPBImpl extends DatabaseLockPayload implements PayloadPB {
  private DatabaseLockPayloadProto proto = DatabaseLockPayloadProto.getDefaultInstance();
  private DatabaseLockPayloadProto.Builder builder = null;
  private boolean viaProto = false;

  public DatabaseLockPayloadPBImpl() {
    builder = DatabaseLockPayloadProto.newBuilder();
  }

  public DatabaseLockPayloadPBImpl(DatabaseLockPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  @Override
  public DatabaseLockPayloadProto getProto() {
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
      builder = DatabaseLockPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public DatabaseReference getDatabase() {
    DatabaseLockPayloadProtoOrBuilder p = viaProto ? proto : builder;
    return new DatabaseReferencePBImpl(p.getDatabase());
  }

  @Override
  public void setDatabase(DatabaseReference db) {
    maybeInitBuilder();
    builder.setDatabase(((DatabaseReferencePBImpl) db).getProto());
  }

  @Override
  public Long getMillis() {
    DatabaseLockPayloadProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasMillis())
      return null;
    return p.getMillis();
  }

  @Override
  public void setMillis(Long  millis) {
    maybeInitBuilder();
    if (millis == null) {
      builder.clearMillis();
      return;
    }
    builder.setMillis(millis);
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = DatabaseLockPayloadProto.parseFrom(data);
    viaProto = true;
  }
}
