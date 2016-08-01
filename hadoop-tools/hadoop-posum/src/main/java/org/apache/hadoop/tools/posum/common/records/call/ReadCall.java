package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

/**
 * Created by ane on 7/29/16.
 */
public abstract class ReadCall<T extends Payload> extends GeneralDatabaseCall<T> {

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection type);

    @Override
    protected void prepare() {
        dataStore.lockForRead(getEntityDB());
    }

    @Override
    protected void wrapUp() {
        dataStore.unlockForRead(getEntityDB());
    }

}
