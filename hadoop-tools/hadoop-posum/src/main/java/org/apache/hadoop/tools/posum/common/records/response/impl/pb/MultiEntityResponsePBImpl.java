package org.apache.hadoop.tools.posum.common.records.response.impl.pb;

import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.yarn.proto.POSUMProtos;

/**
 * Created by ane on 3/20/16.
 */
public class MultiEntityResponsePBImpl extends SimpleResponsePBImpl<MultiEntityPayload> {

    public MultiEntityResponsePBImpl() {
        super();
    }

    public MultiEntityResponsePBImpl(POSUMProtos.SimpleResponseProto proto) {
        super(proto);
    }

}
