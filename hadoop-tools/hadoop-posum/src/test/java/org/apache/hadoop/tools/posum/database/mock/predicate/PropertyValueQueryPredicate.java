package org.apache.hadoop.tools.posum.database.mock.predicate;

import org.apache.hadoop.tools.posum.common.records.call.query.PropertyValueQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.tools.posum.common.util.Utils.safeEquals;

/**
 * Created by ane on 8/14/16.
 */
class PropertyValueQueryPredicate extends QueryPredicate<PropertyValueQuery> {

    PropertyValueQueryPredicate(PropertyValueQuery query) {
        super(query);
        checkedProperties = Collections.singleton(query.getProperty().getName());
    }

    @Override
    public boolean check(GeneralDataEntity entity, Map<String, Method> propertyReaders) throws InvocationTargetException, IllegalAccessException {
        SimplePropertyPayload property = query.getProperty();
        Object value = propertyReaders.get(property.getName()).invoke(entity);
        switch (query.getType()) {
            case IS:
                return safeEquals(value, property.getValue());
            case IS_NOT:
                return !safeEquals(value, property.getValue());
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
