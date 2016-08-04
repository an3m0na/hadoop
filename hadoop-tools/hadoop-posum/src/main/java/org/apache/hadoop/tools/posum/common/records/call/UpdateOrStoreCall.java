package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.GeneralDataEntity;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 7/30/16.
 */
public abstract class UpdateOrStoreCall extends WriteToCollectionCall {
    public static UpdateOrStoreCall newInstance(DataEntityCollection collection, GeneralDataEntity object) {
        UpdateOrStoreCall call = Records.newRecord(UpdateOrStoreCall.class);
        call.setEntityCollection(collection);
        call.setEntity(object);
        return call;
    }

    public static UpdateOrStoreCall newInstance(DataEntityDB db, DataEntityCollection collection, GeneralDataEntity object) {
        UpdateOrStoreCall call = newInstance(collection, object);
        call.setDatabase(db);
        return call;
    }

    @Override
    public SimplePropertyPayload execute(DataStore dataStore) {
        return SimplePropertyPayload.newInstance(
                "upsertedId",
                SimplePropertyPayload.PropertyType.BOOL,
                dataStore.updateOrStore(getDatabase(), getEntityCollection(), getEntity())
        );
    }
}
