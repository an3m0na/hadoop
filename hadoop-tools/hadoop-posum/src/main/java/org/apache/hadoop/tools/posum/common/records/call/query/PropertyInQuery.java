package org.apache.hadoop.tools.posum.common.records.call.query;

import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 8/13/16.
 */
public abstract class PropertyInQuery implements DatabaseQuery {

    protected static PropertyInQuery newInstance(String propertyName, List<Object> propertyValues) {
        PropertyInQuery query = Records.newRecord(PropertyInQuery.class);
        query.setPropertyName(propertyName);
        query.setValues(propertyValues);
        return query;
    }

    public abstract String getPropertyName();

    public abstract void setPropertyName(String propertyName);

    public abstract List<Object> getValues();

    public abstract void setValues(List<Object> values);
}
