package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseProto;
import org.apache.hadoop.yarn.proto.PosumProtos.DatabaseProtoOrBuilder;

public class DatabaseReferencePBImpl extends DatabaseReference {

  private DatabaseProto proto = DatabaseProto.getDefaultInstance();
  private DatabaseProto.Builder builder = null;
  private boolean viaProto = false;


  public DatabaseReferencePBImpl() {
    builder = DatabaseProto.newBuilder();
  }

  public DatabaseReferencePBImpl(DatabaseProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public DatabaseProto getProto() {
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
      builder = DatabaseProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public DatabaseReference.Type getType() {
    DatabaseProtoOrBuilder p = viaProto ? proto : builder;
    return DatabaseReference.Type.valueOf(p.getType().name().substring("DB_".length()));
  }

  @Override
  public void setType(DatabaseReference.Type type) {
    maybeInitBuilder();
    builder.setType(DatabaseProto.DatabaseTypeProto.valueOf("DB_" + type.name()));
  }

  @Override
  public String getView() {
    DatabaseProtoOrBuilder p = viaProto ? proto : builder;
    return p.hasView() ? p.getView() : null;
  }

  @Override
  public void setView(String view) {
    maybeInitBuilder();
    if (view != null)
      builder.setView(view);
  }
}
