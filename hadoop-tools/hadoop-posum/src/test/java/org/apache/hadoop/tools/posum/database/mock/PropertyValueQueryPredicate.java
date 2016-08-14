package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.query.PropertyValueQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static org.apache.hadoop.tools.posum.common.util.Utils.safeEquals;

/**
 * Created by ane on 8/14/16.
 */
class PropertyValueQueryPredicate extends QueryPredicate {

    private final PropertyValueQuery query;

    PropertyValueQueryPredicate(Map<String, Method> propertyReaders, PropertyValueQuery query) {
        super(propertyReaders);
        this.query = query;
    }

    @Override
    public boolean check(GeneralDataEntity entity) throws InvocationTargetException, IllegalAccessException {
        SimplePropertyPayload property = query.getProperty();
        Object value = propertyReaders.get(property.getName()).invoke(entity);
        switch (query.getType()) {
            case IS:
                if (safeEquals(value, property.getValue()))
                    return true;
            case LESS:
                return property.getValue() != null && ((Comparable) property.getValue()).compareTo(value) > 0;
            case LESS_OR_EQUAL:
                return property.getValue() != null && ((Comparable) property.getValue()).compareTo(value) >= 0;
            case GREATER:
                return property.getValue() != null && ((Comparable) property.getValue()).compareTo(value) < 0;
            case GREATER_OR_EQUAL:
                return property.getValue() != null && ((Comparable) property.getValue()).compareTo(value) <= 0;
            default:
                throw new PosumException("PropertyValue query type not recognized: " + query.getType());

        }

    }
}
