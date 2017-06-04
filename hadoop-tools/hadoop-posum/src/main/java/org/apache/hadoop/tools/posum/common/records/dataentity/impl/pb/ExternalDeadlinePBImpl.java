package org.apache.hadoop.tools.posum.common.records.dataentity.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.yarn.proto.PosumProtos.ExternalDeadlineProto;
import org.apache.hadoop.yarn.proto.PosumProtos.ExternalDeadlineProtoOrBuilder;

public class ExternalDeadlinePBImpl extends GeneralDataEntityPBImpl<ExternalDeadline, ExternalDeadlineProto, ExternalDeadlineProto.Builder>
  implements ExternalDeadline {

  public ExternalDeadlinePBImpl() {
  }

  public ExternalDeadlinePBImpl(ExternalDeadlineProto proto) {
    super(proto);
  }

  @Override
  void initBuilder() {
    builder = viaProto ? ExternalDeadlineProto.newBuilder(proto) : ExternalDeadlineProto.newBuilder();
  }

  @Override
  void buildProto() {
    proto = builder.build();
  }

  @Override
  public ExternalDeadline parseToEntity(ByteString data) throws InvalidProtocolBufferException {
    this.proto = ExternalDeadlineProto.parseFrom(data);
    viaProto = true;
    return this;
  }

  @Override
  public String getId() {
    ExternalDeadlineProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasId())
      return null;
    return p.getId();
  }

  @Override
  public void setId(String id) {
    maybeInitBuilder();
    if (id == null) {
      builder.clearId();
      return;
    }
    builder.setId(id);
  }

  @Override
  public Long getDeadline() {
    ExternalDeadlineProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasDeadline())
      return null;
    return p.getDeadline();

  }

  @Override
  public void setDeadline(Long deadline) {
    maybeInitBuilder();
    if (deadline == null) {
      builder.clearDeadline();
      return;
    }
    builder.setDeadline(deadline);
  }

  @Override
  public Long getLastUpdated() {
    ExternalDeadlineProtoOrBuilder p = viaProto ? proto : builder;
    if (!p.hasLastUpdated())
      return null;
    return p.getLastUpdated();
  }

  @Override
  public void setLastUpdated(Long timestamp) {
    maybeInitBuilder();
    if (timestamp == null) {
      builder.clearLastUpdated();
      return;
    }
    builder.setLastUpdated(timestamp);
  }

  @Override
  public ExternalDeadline copy() {
    return new ExternalDeadlinePBImpl(getProto());
  }

}
