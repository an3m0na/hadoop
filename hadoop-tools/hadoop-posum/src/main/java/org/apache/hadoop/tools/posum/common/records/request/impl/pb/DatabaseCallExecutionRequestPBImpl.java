package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.call.impl.pb.DatabaseCallWrapperPBImpl;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DatabaseReferencePBImpl;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.tools.posum.common.records.request.DatabaseCallExecutionRequest;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseCallExecutionRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseCallExecutionRequestProtoOrBuilder;

public class DatabaseCallExecutionRequestPBImpl extends DatabaseCallExecutionRequest implements PayloadPB {
  private DatabaseCallExecutionRequestProto proto = DatabaseCallExecutionRequestProto.getDefaultInstance();
  private DatabaseCallExecutionRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public DatabaseCallExecutionRequestPBImpl() {
    builder = DatabaseCallExecutionRequestProto.newBuilder();
  }

  public DatabaseCallExecutionRequestPBImpl(DatabaseCallExecutionRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public DatabaseCallExecutionRequestProto getProto() {
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
      builder = DatabaseCallExecutionRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  public void setCall(DatabaseCall call) {
    maybeInitBuilder();
    if (call == null) {
      builder.clearCall();
      return;
    }
    builder.setCall(new DatabaseCallWrapperPBImpl(call).getProto());
  }

  public DatabaseCall getCall() {
    DatabaseCallExecutionRequestProtoOrBuilder p = viaProto ? proto : builder;
    return new DatabaseCallWrapperPBImpl(p.getCall()).getCall();
  }


  @Override
  public DatabaseReference getDatabase() {
    DatabaseCallExecutionRequestProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDb())
      return null;
    return new DatabaseReferencePBImpl(p.getDb());
  }

  @Override
  public void setDatabase(DatabaseReference db) {
    maybeInitBuilder();
    if (db == null) {
      builder.clearDb();
      return;
    }
    builder.setDb(((DatabaseReferencePBImpl) db).getProto());
  }

  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = DatabaseCallExecutionRequestProto.parseFrom(data);
    viaProto = true;
  }
}
