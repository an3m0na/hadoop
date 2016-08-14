package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.query.CompositionQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyValueQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Created by ane on 8/14/16.
 */
public abstract class QueryPredicate {

    protected final Map<String, Method> propertyReaders;
    private static QueryPredicate tautology = new QueryPredicate(null) {
        @Override
        public boolean check(GeneralDataEntity entity) {
            return true;
        }
    };

    protected QueryPredicate(Map<String, Method> propertyReaders) {
        this.propertyReaders = propertyReaders;
    }

    public abstract boolean check(GeneralDataEntity entity) throws InvocationTargetException, IllegalAccessException;

    public static QueryPredicate fromQuery(CompositionQuery query, Map<String, Method> propertyReaders) {
        if (query == null)
            return tautology;
        return new CompositionQueryPredicate(propertyReaders, query);
    }

    public static QueryPredicate fromQuery(PropertyValueQuery query, Map<String, Method> propertyReaders) {
        if (query == null)
            return tautology;
        return new PropertyValueQueryPredicate(propertyReaders, query);
    }

    public static QueryPredicate fromQuery(DatabaseQuery query, Map<String, Method> propertyReaders) {
        if (query == null)
            return tautology;
        throw new PosumException("Query type not recognized: " + query.getClass());
    }
}
