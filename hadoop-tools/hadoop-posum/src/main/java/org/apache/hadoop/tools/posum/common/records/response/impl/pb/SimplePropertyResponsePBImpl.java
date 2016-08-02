package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class SimplePropertyResponsePBImpl extends SimpleResponsePBImpl<SimplePropertyPayload> {

    public SimplePropertyResponsePBImpl() {
        super();
    }

    public SimplePropertyResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }
}
