package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;

abstract class ThreePhaseDatabaseCallImpl<T extends Payload> implements ThreePhaseDatabaseCall<T> {

  @Override
  public T executeCall(LockBasedDataStore dataStore, DatabaseReference db) {
    prepare(dataStore, db);
    try {
      T ret = execute(dataStore, db);
      commit(dataStore, db);
      return ret;
    } catch (Exception e) {
      rollBack(dataStore, db);
      throw e;
    }
  }
}
