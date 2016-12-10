package org.apache.hadoop.tools.posum.data.mock.data.predicate;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyValueQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;


class TautologyPredicate extends QueryPredicate<DatabaseQuery> {

    TautologyPredicate(PropertyValueQuery query) {
        super(query);
        checkedProperties = Collections.emptySet();
    }

    @Override
    public boolean check(GeneralDataEntity entity, Map<String, Method> propertyReaders) throws InvocationTargetException, IllegalAccessException {
        return true;

    }

}
