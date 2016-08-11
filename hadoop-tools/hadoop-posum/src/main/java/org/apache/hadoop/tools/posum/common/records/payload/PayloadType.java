package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.tools.posum.common.records.payload.impl.pb.*;
import org.apache.hadoop.tools.posum.common.records.pb.PayloadPB;

/**
 * Created by ane on 8/2/16.
 */
public enum PayloadType {
    VOID(VoidPayloadPBImpl.class),
    SIMPLE_PROPERTY(SimplePropertyPayloadPBImpl.class),
    SINGLE_ENTITY(SingleEntityPayloadPBImpl.class),
    MULTI_ENTITY(MultiEntityPayloadPBImpl.class),
    STRING_STRING_MAP(StringStringMapPayloadPBImpl.class),
    STRING_LIST(StringListPayloadPBImpl.class);

    private Class<? extends PayloadPB> implClass;

    PayloadType(Class<? extends PayloadPB> implClass) {
        this.implClass = implClass;
    }

    public Class<? extends PayloadPB> getImplClass() {
        return implClass;
    }
}
