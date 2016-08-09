package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;

/**
 * Created by ane on 7/29/16.
 */
public abstract class ReadFromCollectionCall<T extends Payload> extends LockBasedDatabaseCallImpl<T> {

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection type);

    @Override
    public void lockDatabase(LockBasedDataStore dataStore, DataEntityDB db) {
        dataStore.lockForRead(db);
    }

    @Override
    public void unlockDatabase(LockBasedDataStore dataStore, DataEntityDB db) {
        dataStore.unlockForRead(db);
    }

}
