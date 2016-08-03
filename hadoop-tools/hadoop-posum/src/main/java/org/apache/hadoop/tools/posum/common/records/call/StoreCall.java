package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 7/29/16.
 */
public abstract class StoreCall extends WriteToCollectionCall {
    public static StoreCall newInstance(DataEntityCollection collection, GeneralDataEntity object) {
        StoreCall call = Records.newRecord(StoreCall.class);
        call.setEntityCollection(collection);
        call.setEntity(object);
        return call;
    }

    public static StoreCall newInstance(DataEntityDB db, DataEntityCollection collection, GeneralDataEntity object) {
        StoreCall call = newInstance(collection, object);
        call.setDatabase(db);
        return call;
    }

    @Override
    public SimplePropertyPayload execute() {
        return SimplePropertyPayload.newInstance(
                "id",
                SimplePropertyPayload.PropertyType.STRING,
                dataStore.store(getDatabase(), getEntityCollection(), getEntity())
        );
    }

}
