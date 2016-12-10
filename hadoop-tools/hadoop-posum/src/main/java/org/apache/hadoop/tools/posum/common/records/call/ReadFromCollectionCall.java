package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;

public abstract class ReadFromCollectionCall<T extends Payload> extends LockBasedDatabaseCallImpl<T> {

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection type);

    @Override
    public void lockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
        dataStore.lockForRead(db);
    }

    @Override
    public void unlockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
        dataStore.unlockForRead(db);
    }

}
