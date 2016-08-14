package org.apache.hadoop.tools.posum.common.records.call.query;

import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 8/13/16.
 */
public abstract class PropertyValueQuery extends DatabaseQuery {
    public enum Type {
        IS, LESS, LESS_OR_EQUAL, GREATER, GREATER_OR_EQUAL
    }

    protected static PropertyValueQuery newInstance(Type type, String propertyName, Object propertyValue) {
        PropertyValueQuery query = Records.newRecord(PropertyValueQuery.class);
        query.setType(type);
        query.setProperty(SimplePropertyPayload.newInstance(propertyName, propertyValue));
        return query;
    }

    public abstract Type getType();

    public abstract void setType(Type type);

    public abstract SimplePropertyPayload getProperty();

    public abstract void setProperty(SimplePropertyPayload property);
}
