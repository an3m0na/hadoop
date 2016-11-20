package org.apache.hadoop.tools.posum.database.mock.predicate;

import org.apache.hadoop.tools.posum.common.records.call.query.PropertyRangeQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Created by ane on 8/14/16.
 */
class PropertyRangeQueryPredicate extends QueryPredicate<PropertyRangeQuery> {

    PropertyRangeQueryPredicate(PropertyRangeQuery query) {
        super(query);
        checkedProperties = Collections.singleton(query.getPropertyName());
    }

    @Override
    public boolean check(GeneralDataEntity entity, Map<String, Method> propertyReaders) throws InvocationTargetException, IllegalAccessException {
        Object value = propertyReaders.get(query.getPropertyName()).invoke(entity);
        switch (query.getType()) {
            case IN:
                return query.getValues().contains(value);
            case NOT_IN:
                return !query.getValues().contains(value);
            default:
                throw new PosumException("PropertyRange query type not recognized: " + query.getType());
        }
    }
}
