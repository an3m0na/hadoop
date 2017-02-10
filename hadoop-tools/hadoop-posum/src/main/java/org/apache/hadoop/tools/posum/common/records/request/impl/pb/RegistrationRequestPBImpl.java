package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.TextFormat;
import org.apache.hadoop.tools.posum.common.records.request.RegistrationRequest;
import org.apache.hadoop.tools.posum.common.util.Utils;
import org.apache.hadoop.yarn.proto.PosumProtos;
import org.apache.hadoop.yarn.proto.PosumProtos.RegistrationRequestProto;
import org.apache.hadoop.yarn.proto.PosumProtos.RegistrationRequestProtoOrBuilder;

public class RegistrationRequestPBImpl extends RegistrationRequest {
  private RegistrationRequestProto proto = RegistrationRequestProto.getDefaultInstance();
  private RegistrationRequestProto.Builder builder = null;
  private boolean viaProto = false;

  public RegistrationRequestPBImpl() {
    builder = RegistrationRequestProto.newBuilder();
  }

  public RegistrationRequestPBImpl(RegistrationRequestProto proto) {
    this.proto = proto;
    viaProto = true;
  }

  public RegistrationRequestProto getProto() {
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
      builder = RegistrationRequestProto.newBuilder(proto);
    }
    viaProto = false;
  }

  @Override
  public String getConnectAddress() {
    RegistrationRequestProtoOrBuilder p = viaProto ? proto : builder;
    return p.getConnectAddress();
  }

  @Override
  public void setConnectAddress(String connectAddress) {
    maybeInitBuilder();
    if (connectAddress != null)
      builder.setConnectAddress(connectAddress);
  }

  @Override
  public Utils.PosumProcess getProcess() {
    RegistrationRequestProtoOrBuilder p = viaProto ? proto : builder;
    return Utils.PosumProcess.valueOf(p.getProcess().name().substring("PROCESS_".length()));
  }

  @Override
  public void setProcess(Utils.PosumProcess process) {
    maybeInitBuilder();
    if (process != null)
      builder.setProcess(PosumProtos.RegistrationRequestProto.PosumProcessProto.valueOf("PROCESS_" + process.name()));
  }


}
