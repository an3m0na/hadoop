package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyValueQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.Set;


/**
 * Created by ane on 8/14/16.
 */
class TautologyPredicate extends QueryPredicate<DatabaseQuery> {

    TautologyPredicate(PropertyValueQuery query) {
        super(query);
    }

    @Override
    public boolean check(GeneralDataEntity entity, Map<String, Method> propertyReaders) throws InvocationTargetException, IllegalAccessException {
        return true;

    }

    @Override
    Set<String> parseRelevantProperties() {
        return Collections.emptySet();
    }
}
