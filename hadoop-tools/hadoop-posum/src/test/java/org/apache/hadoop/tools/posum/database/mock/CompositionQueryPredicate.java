package org.apache.hadoop.tools.posum.database.mock;

import org.apache.hadoop.tools.posum.common.records.call.query.CompositionQuery;
import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.util.PosumException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by ane on 8/14/16.
 */
class CompositionQueryPredicate extends QueryPredicate {

    private final CompositionQuery.Type type;
    private final List<QueryPredicate> innerPredicates;

    CompositionQueryPredicate(Map<String, Method> propertyReaders, CompositionQuery query) {
        super(propertyReaders);
        this.type = query.getType();
        this.innerPredicates = new LinkedList<>();
        for (DatabaseQuery innerQuery : query.getQueries()) {
            innerPredicates.add(fromQuery(innerQuery, propertyReaders));
        }
    }

    @Override
    public boolean check(GeneralDataEntity entity) throws InvocationTargetException, IllegalAccessException {
        switch (type) {
            case AND:
                for (QueryPredicate innerPredicate : innerPredicates) {
                    if (!innerPredicate.check(entity))
                        return false;
                }
                return true;
            case OR:
                for (QueryPredicate innerPredicate : innerPredicates) {
                    if (innerPredicate.check(entity))
                        return true;
                }
                return false;
            default:
                throw new PosumException("Composition query type not recognized: " + type);
        }
    }
}
