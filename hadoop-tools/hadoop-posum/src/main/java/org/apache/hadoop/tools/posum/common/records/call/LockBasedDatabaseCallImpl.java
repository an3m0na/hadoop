package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;

/**
 * Created by ane on 8/2/16.
 */
abstract class LockBasedDatabaseCallImpl<T extends Payload> extends ThreePhaseDatabaseCallImpl<T> {

    @Override
    public void prepare(LockBasedDataStore dataStore, DataEntityDB db) {
        lockDatabase(dataStore, db);
    }

    @Override
    public void commit(LockBasedDataStore dataStore, DataEntityDB db) {
        unlockDatabase(dataStore, db);
    }

    @Override
    public void rollBack(LockBasedDataStore dataStore, DataEntityDB db) {
        unlockDatabase(dataStore, db);
    }

    protected abstract void unlockDatabase(LockBasedDataStore dataStore, DataEntityDB db);

    protected abstract void lockDatabase(LockBasedDataStore dataStore, DataEntityDB db);
}
