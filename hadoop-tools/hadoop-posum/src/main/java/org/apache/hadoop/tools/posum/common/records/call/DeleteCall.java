package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;

/**
 * Created by ane on 7/29/16.
 */
public abstract class DeleteCall extends LockBasedDatabaseCallImpl<VoidPayload> {

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection collection);

    @Override
    public void lockDatabase() {
        dataStore.lockForWrite(getDatabase());
    }

    @Override
    public void unlockDatabase() {
        dataStore.unlockForWrite(getDatabase());
    }

}
