package org.apache.hadoop.tools.posum.data.mock.data.predicate;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

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
