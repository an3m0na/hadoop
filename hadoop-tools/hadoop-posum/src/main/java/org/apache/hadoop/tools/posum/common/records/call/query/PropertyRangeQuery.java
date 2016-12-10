package org.apache.hadoop.tools.posum.common.records.call.query;

import org.apache.hadoop.yarn.util.Records;

import java.util.List;

public abstract class PropertyRangeQuery implements DatabaseQuery {

    public enum Type {
        IN, NOT_IN
    }

    protected static PropertyRangeQuery newInstance(String propertyName, Type type, List<?> propertyValues) {
        PropertyRangeQuery query = Records.newRecord(PropertyRangeQuery.class);
        query.setPropertyName(propertyName);
        query.setType(type);
        query.setValues(propertyValues);
        return query;
    }

    public abstract Type getType();

    public abstract void setType(Type type);

    public abstract String getPropertyName();

    public abstract void setPropertyName(String propertyName);

    public abstract <T> List<T> getValues();

    public abstract void setValues(List<?> values);
}
