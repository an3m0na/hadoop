package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;

abstract class ThreePhaseDatabaseCallImpl<T extends Payload> implements ThreePhaseDatabaseCall<T> {
  private static Log logger = LogFactory.getLog(ThreePhaseDatabaseCallImpl.class);

  @Override
  public T executeCall(LockBasedDataStore dataStore, DatabaseReference db) {
    prepare(dataStore, db);
    try {
      T ret = execute(dataStore, db);
      commit(dataStore, db);
      return ret;
    } catch (Exception e) {
      logger.debug("Exception occurred. Rolling back", e);
      rollBack(dataStore, db);
      throw e;
    }
  }
}
