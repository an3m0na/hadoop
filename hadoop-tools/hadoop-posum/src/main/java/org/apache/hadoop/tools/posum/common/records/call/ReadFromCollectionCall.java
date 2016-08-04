package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.DataStore;

/**
 * Created by ane on 7/29/16.
 */
public abstract class ReadFromCollectionCall<T extends Payload> extends LockBasedDatabaseCallImpl<T> {

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection type);

    @Override
    public void lockDatabase(DataStore dataStore) {
        dataStore.lockForRead(getDatabase());
    }

    @Override
    public void unlockDatabase(DataStore dataStore) {
        dataStore.unlockForRead(getDatabase());
    }

}
