package org.apache.hadoop.tools.posum.common.records.payload.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb.DatabaseReferencePBImpl;
import org.apache.hadoop.tools.posum.common.records.payload.DatabaseAlterationPayload;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseAlterationPayloadProto;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseAlterationPayloadProtoOrBuilder;

public class DatabaseAlterationPayloadPBImpl extends DatabaseAlterationPayload implements PayloadPB {
  private DatabaseAlterationPayloadProto proto = DatabaseAlterationPayloadProto.getDefaultInstance();
  private DatabaseAlterationPayloadProto.Builder builder = null;
  private boolean viaProto = false;

  public DatabaseAlterationPayloadPBImpl() {
    builder = DatabaseAlterationPayloadProto.newBuilder();
  }

  public DatabaseAlterationPayloadPBImpl(DatabaseAlterationPayloadProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public DatabaseAlterationPayloadProto getProto() {
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
      builder = DatabaseAlterationPayloadProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public DatabaseReference getSourceDB() {
    DatabaseAlterationPayloadProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasSource())
      return null;
    return new DatabaseReferencePBImpl(p.getSource());
  }

  @Override
  public void setSourceDB(DatabaseReference db) {
    maybeInitBuilder();
    if (db == null) {
      builder.clearSource();
      return;
    }
    builder.setSource(((DatabaseReferencePBImpl) db).getProto());
  }

  @Override
  public DatabaseReference getDestinationDB() {
    DatabaseAlterationPayloadProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDestination())
      return null;
    return new DatabaseReferencePBImpl(p.getDestination());
  }

  @Override
  public void setDestinationDB(DatabaseReference db) {
    maybeInitBuilder();
    if (db == null) {
      builder.clearDestination();
      return;
    }
    builder.setDestination(((DatabaseReferencePBImpl) db).getProto());
  }

  @Override
  public DataEntityCollection getTargetCollection() {
    DatabaseAlterationPayloadProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasTargetCollection())
      return null;
    return DataEntityCollection.valueOf(p.getTargetCollection().name().substring("COLL_".length()));
  }

  @Override
  public void setTargetCollection(DataEntityCollection collection) {
    maybeInitBuilder();
    if (collection == null) {
      builder.clearTargetCollection();
      return;
    }
    builder.setTargetCollection(PosumProtos.EntityCollectionProto.valueOf("COLL_" + collection.name()));
  }


  @Override
  public ByteString getProtoBytes() {
    return getProto().toByteString();
  }

  @Override
  public void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException {
    proto = DatabaseAlterationPayloadProto.parseFrom(data);
    viaProto = true;
  }
}
