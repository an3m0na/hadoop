package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;

abstract class LockBasedDatabaseCallImpl<T extends Payload> extends ThreePhaseDatabaseCallImpl<T> {

    @Override
    public void prepare(LockBasedDataStore dataStore, DatabaseReference db) {
        lockDatabase(dataStore, db);
    }

    @Override
    public void commit(LockBasedDataStore dataStore, DatabaseReference db) {
        unlockDatabase(dataStore, db);
    }

    @Override
    public void rollBack(LockBasedDataStore dataStore, DatabaseReference db) {
        unlockDatabase(dataStore, db);
    }

    protected abstract void unlockDatabase(LockBasedDataStore dataStore, DatabaseReference db);

    protected abstract void lockDatabase(LockBasedDataStore dataStore, DatabaseReference db);
}
