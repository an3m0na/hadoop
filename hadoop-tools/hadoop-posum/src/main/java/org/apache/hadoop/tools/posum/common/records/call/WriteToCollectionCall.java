package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 8/1/16.
 */
public abstract class WriteToCollectionCall extends LockBasedDatabaseCallImpl<SimplePropertyPayload> {

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection collection);

    public abstract GeneralDataEntity getEntity();

    public abstract void setEntity(GeneralDataEntity entity);

    @Override
    public void lockDatabase(DataStore dataStore) {
        dataStore.lockForWrite(getDatabase());
    }

    @Override
    public void unlockDatabase(DataStore dataStore) {
        dataStore.unlockForWrite(getDatabase());
    }
}
