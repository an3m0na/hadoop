package org.apache.hadoop.tools.posum.common.records.pb;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Created by ane on 8/1/16.
 */
public interface ByteStringSerializable {
    ByteString getProtoBytes();

    void populateFromProtoBytes(ByteString data) throws InvalidProtocolBufferException;
}
