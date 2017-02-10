package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;

public interface ThreePhaseDatabaseCall<T extends Payload> extends DatabaseCall<T> {
  void prepare(LockBasedDataStore dataStore, DatabaseReference db);

  T execute(LockBasedDataStore dataStore, DatabaseReference db);

  void commit(LockBasedDataStore dataStore, DatabaseReference db);

  void rollBack(LockBasedDataStore dataStore, DatabaseReference db);
}
