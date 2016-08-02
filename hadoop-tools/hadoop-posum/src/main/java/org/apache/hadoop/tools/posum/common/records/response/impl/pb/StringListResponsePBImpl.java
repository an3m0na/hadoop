package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class StringListResponsePBImpl extends SimpleResponsePBImpl<StringListPayload> {

    public StringListResponsePBImpl() {
        super();
    }

    public StringListResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }
}
