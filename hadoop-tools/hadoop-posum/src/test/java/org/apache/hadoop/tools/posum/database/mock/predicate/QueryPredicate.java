package org.apache.hadoop.tools.posum.database.mock.predicate;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by ane on 8/14/16.
 */
public abstract class QueryPredicate<T extends DatabaseQuery> {

    final T query;
    Set<String> checkedProperties;

    QueryPredicate(T query) {
        this.query = query;
    }

    public abstract boolean check(GeneralDataEntity entity, Map<String, Method> propertyReaders) throws InvocationTargetException, IllegalAccessException;

    public Set<String> getCheckedProperties() {
        return checkedProperties;
    }
}
