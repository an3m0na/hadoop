package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

public abstract class UpdateOrStoreCall extends WriteToCollectionCall<SimplePropertyPayload> {
    public static UpdateOrStoreCall newInstance(DataEntityCollection collection, GeneralDataEntity object) {
        UpdateOrStoreCall call = Records.newRecord(UpdateOrStoreCall.class);
        call.setEntityCollection(collection);
        call.setEntity(object);
        return call;
    }

    public abstract GeneralDataEntity getEntity();

    public abstract void setEntity(GeneralDataEntity entity);

    @Override
    public SimplePropertyPayload execute(LockBasedDataStore dataStore, DataEntityDB db) {
        return SimplePropertyPayload.newInstance("upsertedId",
                dataStore.updateOrStore(db, getEntityCollection(), getEntity()));
    }
}
