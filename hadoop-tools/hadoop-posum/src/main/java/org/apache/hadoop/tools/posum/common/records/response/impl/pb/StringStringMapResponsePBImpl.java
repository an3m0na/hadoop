package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import org.apache.hadoop.tools.posum.common.records.payload.StringStringMapPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class StringStringMapResponsePBImpl extends SimpleResponsePBImpl<StringStringMapPayload> {

    public StringStringMapResponsePBImpl() {
        super();
    }

    public StringStringMapResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }
}
