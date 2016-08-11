package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;

/**
 * Created by ane on 7/29/16.
 */
abstract class ThreePhaseDatabaseCallImpl<T extends Payload> implements ThreePhaseDatabaseCall<T> {

    @Override
    public T executeCall(LockBasedDataStore dataStore, DataEntityDB db) {
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
