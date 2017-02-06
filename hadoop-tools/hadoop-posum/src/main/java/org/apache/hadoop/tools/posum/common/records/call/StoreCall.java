package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

public abstract class StoreCall extends WriteToCollectionCall<SimplePropertyPayload> {
    public static StoreCall newInstance(DataEntityCollection collection, GeneralDataEntity object) {
        StoreCall call = Records.newRecord(StoreCall.class);
        call.setEntityCollection(collection);
        call.setEntity(object);
        return call;
    }

    public abstract GeneralDataEntity getEntity();

    public abstract void setEntity(GeneralDataEntity entity);

    @Override
    public SimplePropertyPayload execute(LockBasedDataStore dataStore, DatabaseReference db) {
        return SimplePropertyPayload.newInstance("id", dataStore.store(db, getEntityCollection(), getEntity()));
    }

}
