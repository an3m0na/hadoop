package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class VoidResponsePBImpl extends SimpleResponsePBImpl<VoidPayload> {

    public VoidResponsePBImpl() {
        super();
    }

    public VoidResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }
}
