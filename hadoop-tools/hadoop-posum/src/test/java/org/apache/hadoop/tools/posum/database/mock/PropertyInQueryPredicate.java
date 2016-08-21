package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.query.PropertyInQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by ane on 8/14/16.
 */
class PropertyInQueryPredicate extends QueryPredicate {

    private final PropertyInQuery query;

    PropertyInQueryPredicate(Map<String, Method> propertyReaders, PropertyInQuery query) {
        super(propertyReaders);
        this.query = query;
    }

    @Override
    public boolean check(GeneralDataEntity entity) throws InvocationTargetException, IllegalAccessException {
        Object value = propertyReaders.get(query.getPropertyName()).invoke(entity);
        return query.getValues().contains(value);
    }
}
