package org.apache.hadoop.tools.posum.data.mock.data.predicate;

import org.apache.hadoop.tools.posum.common.records.call.query.CompositionQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyRangeQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.PropertyValueQuery;
import org.apache.hadoop.tools.posum.common.util.PosumException;

public class QueryPredicateFactory {

    public static QueryPredicate<? extends DatabaseQuery> fromQuery(DatabaseQuery query) {
        if (query == null)
            return new TautologyPredicate(null);
        if (query instanceof PropertyValueQuery)
            return new PropertyValueQueryPredicate((PropertyValueQuery) query);
        if (query instanceof CompositionQuery)
            return new CompositionQueryPredicate((CompositionQuery) query);
        if (query instanceof PropertyRangeQuery)
            return new PropertyRangeQueryPredicate((PropertyRangeQuery) query);
        throw new PosumException("Query type not recognized: " + query.getClass());
    }
}
