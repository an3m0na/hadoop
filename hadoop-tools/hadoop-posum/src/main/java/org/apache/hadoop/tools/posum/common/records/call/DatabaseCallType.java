package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.call.impl.pb.*;
import org.apache.hadoop.tools.posum.common.records.payload.PayloadType;

/**
 * Created by ane on 8/1/16.
 */
public enum DatabaseCallType {
    FIND_BY_ID(FindByIdCallPBImpl.class, PayloadType.SINGLE_ENTITY),
    FIND_BY_PARAMS(FindByParamsCallPBImpl.class, PayloadType.MULTI_ENTITY),
    DELETE_BY_ID(DeleteByIdCallPBImpl.class, PayloadType.VOID),
    DELETE_BY_PARAMS(DeleteByParamsCallPBImpl.class, PayloadType.VOID),
    STORE(StoreCallPBImpl.class, PayloadType.SIMPLE_PROPERTY),
    UPDATE_OR_STORE(UpdateOrStoreCallPBImpl.class, PayloadType.SIMPLE_PROPERTY);

    private Class<? extends DatabaseCall> mappedClass;
    private PayloadType payloadType;

    DatabaseCallType(Class<? extends DatabaseCall> mappedClass, PayloadType payloadType) {
        this.mappedClass = mappedClass;
        this.payloadType = payloadType;
    }

    public Class<? extends DatabaseCall> getMappedClass() {
        return mappedClass;
    }

    public static DatabaseCallType fromMappedClass(Class<? extends DatabaseCall> requiredClass) {
        for (DatabaseCallType type : values()) {
            if (type.getMappedClass().equals(requiredClass))
                return type;
        }
        return null;
    }

    public PayloadType getPayloadType() {
        return payloadType;
    }
}
