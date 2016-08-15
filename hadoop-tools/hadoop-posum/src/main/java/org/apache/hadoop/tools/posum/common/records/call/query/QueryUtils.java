package org.apache.hadoop.tools.posum.common.records.call.query;

import java.util.Map;

/**
 * Created by ane on 8/13/16.
 */
public class QueryUtils {

    public static DatabaseQuery withParams(Map<String, Object> params) {
        DatabaseQuery[] queries = new DatabaseQuery[params.size()];
        int i = 0;
        for (Map.Entry<String, Object> paramEntry : params.entrySet()) {
            queries[i++] = is(paramEntry.getKey(), paramEntry.getValue());
        }
        return CompositionQuery.newInstance(CompositionQuery.Type.AND, queries);
    }

    public static DatabaseQuery and(DatabaseQuery... queries) {
        return CompositionQuery.newInstance(CompositionQuery.Type.AND, queries);
    }

    public static DatabaseQuery or(DatabaseQuery... queries) {
        return CompositionQuery.newInstance(CompositionQuery.Type.OR, queries);
    }

    public static DatabaseQuery is(String propertyName, Object propertyValue) {
        return PropertyValueQuery.newInstance(PropertyValueQuery.Type.IS, propertyName, propertyValue);
    }

    public static DatabaseQuery lessThan(String propertyName, Object propertyValue) {
        return PropertyValueQuery.newInstance(PropertyValueQuery.Type.LESS, propertyName, propertyValue);
    }

    public static DatabaseQuery lessThanOrEqual(String propertyName, Object propertyValue) {
        return PropertyValueQuery.newInstance(PropertyValueQuery.Type.LESS_OR_EQUAL, propertyName, propertyValue);
    }

    public static DatabaseQuery greaterThan(String propertyName, Object propertyValue) {
        return PropertyValueQuery.newInstance(PropertyValueQuery.Type.GREATER, propertyName, propertyValue);
    }

    public static DatabaseQuery greaterThanOrEqual(String propertyName, Object propertyValue) {
        return PropertyValueQuery.newInstance(PropertyValueQuery.Type.GREATER_OR_EQUAL, propertyName, propertyValue);
    }

}
