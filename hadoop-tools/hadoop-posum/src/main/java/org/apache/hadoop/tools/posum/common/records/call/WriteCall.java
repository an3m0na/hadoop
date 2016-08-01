package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 8/1/16.
 */
public abstract class WriteCall extends GeneralDatabaseCall<SimplePropertyPayload> {

    public static StoreCall newInstance(DataEntityDB db, DataEntityCollection collection, GeneralDataEntity object) {
        StoreCall call = Records.newRecord(StoreCall.class);
        call.setEntityDB(db);
        call.setEntityCollection(collection);
        call.setEntity(object);
        return call;
    }

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract DataEntityCollection getEntityCollection();

    public abstract void setEntityCollection(DataEntityCollection collection);

    public abstract GeneralDataEntity getEntity();

    public abstract void setEntity(GeneralDataEntity entity);

    @Override
    protected void prepare() {
        dataStore.lockForWrite(getEntityDB());
    }

    @Override
    protected void wrapUp() {
        dataStore.unlockForWrite(getEntityDB());
    }
}
