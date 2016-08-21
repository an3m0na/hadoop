package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 * Created by ane on 8/14/16.
 */
public abstract class QueryPredicate<T extends DatabaseQuery> {

    final T query;
    private final Set<String> checkedProperties;

    QueryPredicate(T query) {
        this.query = query;
        checkedProperties = parseRelevantProperties();
    }

    public abstract boolean check(GeneralDataEntity entity, Map<String, Method> propertyReaders) throws InvocationTargetException, IllegalAccessException;

    abstract Set<String> parseRelevantProperties();

    Set<String> getCheckedProperties() {
        return checkedProperties;
    }
}
