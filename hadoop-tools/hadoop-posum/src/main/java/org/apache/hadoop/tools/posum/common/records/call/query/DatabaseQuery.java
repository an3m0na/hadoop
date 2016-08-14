package org.apache.hadoop.tools.posum.common.records.call.query;

/**
 * Created by ane on 8/13/16.
 */
public class DatabaseQuery {
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
