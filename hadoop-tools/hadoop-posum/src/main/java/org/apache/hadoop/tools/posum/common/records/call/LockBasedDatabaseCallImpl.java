package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.DataStore;

/**
 * Created by ane on 8/2/16.
 */
abstract class LockBasedDatabaseCallImpl<T extends Payload> extends ThreePhaseDatabaseCallImpl<T> {

    @Override
    public void prepare(DataStore dataStore, DataEntityDB db) {
        lockDatabase(dataStore, db);
    }

    @Override
    public void commit(DataStore dataStore, DataEntityDB db) {
        unlockDatabase(dataStore, db);
    }

    @Override
    public void rollBack(DataStore dataStore, DataEntityDB db) {
        unlockDatabase(dataStore, db);
    }

    protected abstract void unlockDatabase(DataStore dataStore, DataEntityDB db);

    protected abstract void lockDatabase(DataStore dataStore, DataEntityDB db);
}
