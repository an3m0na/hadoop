package org.apache.hadoop.tools.posum.common.records.pb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

@JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public interface PayloadPB<T> extends Payload {
  @JsonIgnore
  @org.codehaus.jackson.annotate.JsonIgnore
  T getProto();

  @JsonIgnore
  @org.codehaus.jackson.annotate.JsonIgnore
  ByteString getProtoBytes();

  void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException;
}
