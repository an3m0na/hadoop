package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

/**
 * Created by ane on 7/29/16.
 */
public abstract class ReadFromCollectionCall<T extends Payload> extends LockBasedDatabaseCallImpl<T> {

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection type);

    @Override
    public void lockDatabase() {
        dataStore.lockForRead(getDatabase());
    }

    @Override
    public void unlockDatabase() {
        dataStore.unlockForRead(getDatabase());
    }

}
