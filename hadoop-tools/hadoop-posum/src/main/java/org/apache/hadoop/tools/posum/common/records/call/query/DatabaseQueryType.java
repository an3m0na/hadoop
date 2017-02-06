package org.apache.hadoop.tools.posum.common.records.call.query;

import org.apache.hadoop.tools.posum.common.records.call.query.impl.pb.CompositionQueryPBImpl;
import org.apache.hadoop.tools.posum.common.records.call.query.impl.pb.PropertyRangeQueryPBImpl;
import org.apache.hadoop.tools.posum.common.records.call.query.impl.pb.PropertyValueQueryPBImpl;

public enum DatabaseQueryType {
    COMPOSITION(CompositionQueryPBImpl.class),
    PROPERTY_VALUE(PropertyValueQueryPBImpl.class),
    PROPERTY_IN(PropertyRangeQueryPBImpl.class);

    private Class<? extends DatabaseQuery> mappedClass;

    DatabaseQueryType(Class<? extends DatabaseQuery> mappedClass) {
        this.mappedClass = mappedClass;
    }

    public Class<? extends DatabaseQuery> getMappedClass() {
        return mappedClass;
    }

    public static DatabaseQueryType fromMappedClass(Class<? extends DatabaseQuery> requiredClass) {
        for (DatabaseQueryType type : values()) {
            if (type.getMappedClass().equals(requiredClass))
                return type;
        }
        return null;
    }
}
