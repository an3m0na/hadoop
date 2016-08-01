package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.call.impl.pb.*;

/**
 * Created by ane on 8/1/16.
 */
public enum DatabaseCallType {
    FIND_BY_ID(FindByIdCallPBImpl.class),
    FIND_BY_PARAMS(FindByParamsCallPBImpl.class),
    DELETE_BY_ID(DeleteByIdCallPBImpl.class),
    DELETE_BY_PARAMS(DeleteByParamsCallPBImpl.class),
    STORE(StoreCallPBImpl.class),
    UPDATE_OR_STORE(UpdateOrStoreCallPBImpl.class);

    private Class<? extends GeneralDatabaseCall> mappedClass;

    DatabaseCallType(Class<? extends GeneralDatabaseCall> mappedClass) {
        this.mappedClass = mappedClass;
    }

    public Class<? extends GeneralDatabaseCall> getMappedClass() {
        return mappedClass;
    }

    public static DatabaseCallType fromMappedClass(Class<? extends GeneralDatabaseCall> requiredClass) {
        for (DatabaseCallType type : values()) {
            if (type.getMappedClass().equals(requiredClass))
                return type;
        }
        return null;
    }

}
