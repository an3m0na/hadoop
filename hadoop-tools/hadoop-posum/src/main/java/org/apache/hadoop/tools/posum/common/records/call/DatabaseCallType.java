package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.call.impl.pb.*;
import org.apache.hadoop.tools.posum.common.records.payload.PayloadType;

public enum DatabaseCallType {
    FIND_BY_ID(FindByIdCallPBImpl.class, PayloadType.SINGLE_ENTITY),
    DELETE_BY_ID(DeleteByIdCallPBImpl.class, PayloadType.VOID),
    DELETE_BY_PARAMS(DeleteByQueryCallPBImpl.class, PayloadType.VOID),
    STORE(StoreCallPBImpl.class, PayloadType.SIMPLE_PROPERTY),
    UPDATE_OR_STORE(UpdateOrStoreCallPBImpl.class, PayloadType.SIMPLE_PROPERTY),
    JOB_FOR_APP(JobForAppCallPBImpl.class, PayloadType.SINGLE_ENTITY),
    SAVE_FLEX_FIELDS(SaveJobFlexFieldsCallPBImpl.class, PayloadType.VOID),
    IDS_BY_PARAMS(IdsByQueryCallPBImpl.class, PayloadType.STRING_LIST),
    TRANSACTION(TransactionCallPBImpl.class, PayloadType.VOID),
    STORE_LOG(StoreLogCallPBImpl.class, PayloadType.SIMPLE_PROPERTY),
    FIND_BY_QUERY(FindByQueryCallPBImpl.class, PayloadType.MULTI_ENTITY),
    DOCUMENTS_BY_QUERY(RawDocumentsByQueryCallPBImpl.class, PayloadType.SIMPLE_PROPERTY),
    STORE_ALL(StoreAllCallPBImpl.class, PayloadType.VOID);

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
