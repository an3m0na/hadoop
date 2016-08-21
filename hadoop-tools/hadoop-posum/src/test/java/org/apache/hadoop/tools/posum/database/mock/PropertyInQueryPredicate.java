package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.query.PropertyInQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by ane on 8/14/16.
 */
class PropertyInQueryPredicate extends QueryPredicate<PropertyInQuery> {

    PropertyInQueryPredicate(PropertyInQuery query) {
        super(query);
    }

    @Override
    public boolean check(GeneralDataEntity entity, Map<String, Method> propertyReaders) throws InvocationTargetException, IllegalAccessException {
        Object value = propertyReaders.get(query.getPropertyName()).invoke(entity);
        return query.getValues().contains(value);
    }

    @Override
    Set<String> parseRelevantProperties() {
        return Collections.singleton(query.getPropertyName());
    }
}
