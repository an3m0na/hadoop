package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.query.CompositionQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by ane on 8/14/16.
 */
class CompositionQueryPredicate extends QueryPredicate<CompositionQuery> {

    private final CompositionQuery.Type type;
    private final List<QueryPredicate<? extends DatabaseQuery>> innerPredicates;

    CompositionQueryPredicate(CompositionQuery query) {
        super(query);
        this.type = query.getType();
        this.innerPredicates = new LinkedList<>();
        for (DatabaseQuery innerQuery : query.getQueries()) {
            innerPredicates.add(QueryPredicateFactory.fromQuery(innerQuery));
        }
    }

    @Override
    public boolean check(GeneralDataEntity entity, Map<String, Method> propertyReaders) throws InvocationTargetException, IllegalAccessException {
        switch (type) {
            case AND:
                for (QueryPredicate innerPredicate : innerPredicates) {
                    if (!innerPredicate.check(entity, propertyReaders))
                        return false;
                }
                return true;
            case OR:
                for (QueryPredicate innerPredicate : innerPredicates) {
                    if (innerPredicate.check(entity, propertyReaders))
                        return true;
                }
                return false;
            default:
                throw new PosumException("Composition query type not recognized: " + type);
        }
    }

    @Override
    Set<String> parseRelevantProperties() {
            Set<String> ret = new HashSet<>(query.getQueries().size());
            for (QueryPredicate<? extends DatabaseQuery> predicate : innerPredicates) {
                ret.addAll(predicate.parseRelevantProperties());
            }
            return ret;
    }
}
