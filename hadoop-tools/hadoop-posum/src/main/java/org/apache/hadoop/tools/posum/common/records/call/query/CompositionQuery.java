package org.apache.hadoop.tools.posum.common.records.call.query;

import org.apache.hadoop.yarn.util.Records;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ane on 8/13/16.
 */
public abstract class CompositionQuery implements DatabaseQuery {

    public enum Type {
        AND, OR
    }

    protected static CompositionQuery newInstance(Type type, DatabaseQuery... queries) {
        CompositionQuery query = Records.newRecord(CompositionQuery.class);
        query.setType(type);
        query.setQueries(Arrays.asList(queries));
        return query;
    }

    public abstract Type getType();

    public abstract void setType(Type type);

    public abstract List<DatabaseQuery> getQueries();

    public abstract void setQueries(List<DatabaseQuery> queries);
}
