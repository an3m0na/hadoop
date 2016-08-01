package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;

/**
 * Created by ane on 7/29/16.
 */
public abstract class DeleteCall extends GeneralDatabaseCall<VoidPayload> {

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection collection);

    @Override
    protected void prepare() {
        dataStore.lockForWrite(getEntityDB());
    }

    @Override
    protected void wrapUp() {
        dataStore.unlockForWrite(getEntityDB());
    }

}
