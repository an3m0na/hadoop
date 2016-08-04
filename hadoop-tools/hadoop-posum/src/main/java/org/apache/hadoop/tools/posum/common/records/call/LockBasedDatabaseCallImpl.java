package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.DataStore;

/**
 * Created by ane on 8/2/16.
 */
abstract class LockBasedDatabaseCallImpl<T extends Payload> extends ThreePhaseDatabaseCallImpl<T> {

    public void prepare(DataStore dataStore) {
        lockDatabase(dataStore);
    }

    public void commit(DataStore dataStore) {
        unlockDatabase(dataStore);
    }

    public void rollBack(DataStore dataStore) {
        unlockDatabase(dataStore);
    }

    protected abstract void unlockDatabase(DataStore dataStore);

    protected abstract void lockDatabase(DataStore dataStore);
}
