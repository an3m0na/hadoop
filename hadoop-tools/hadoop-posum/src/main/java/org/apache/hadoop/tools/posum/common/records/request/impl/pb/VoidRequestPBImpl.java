package org.apache.hadoop.tools.posum.common.records.request.impl.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.yarn.proto.PosumProtos;

public class VoidRequestPBImpl extends SimpleRequestPBImpl<VoidPayload> {

  public VoidRequestPBImpl() {
    super();
  }

  public VoidRequestPBImpl(PosumProtos.SimpleRequestProto proto) {
    super(proto);
  }

  @Override
  public ByteString payloadToBytes(VoidPayload payload) {
    return ByteString.EMPTY;
  }

  @Override
  public VoidPayload bytesToPayload(ByteString data) throws InvalidProtocolBufferException {
    return null;
  }
}
