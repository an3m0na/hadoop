package org.apache.hadoop.tools.posum.common.records.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

public interface PayloadPB extends Payload {
    ByteString getProtoBytes();

    void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException;
}
