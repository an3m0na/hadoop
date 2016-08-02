package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class SingleEntityResponsePBImpl extends SimpleResponsePBImpl<SingleEntityPayload> {

    public SingleEntityResponsePBImpl() {
        super();
    }

    public SingleEntityResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }
}
