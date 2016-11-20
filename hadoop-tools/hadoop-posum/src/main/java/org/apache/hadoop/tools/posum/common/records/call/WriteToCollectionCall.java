package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;

/**
 * Created by ane on 8/1/16.
 */
public abstract class WriteToCollectionCall<T extends Payload> extends LockBasedDatabaseCallImpl<T> {

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection collection);

    @Override
    public void lockDatabase(LockBasedDataStore dataStore, DataEntityDB db) {
        dataStore.lockForWrite(db);
    }

    @Override
    public void unlockDatabase(LockBasedDataStore dataStore, DataEntityDB db) {
        dataStore.unlockForWrite(db);
    }
}
