package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

public abstract class FindByIdCall extends ReadFromCollectionCall<SingleEntityPayload> {

    public static FindByIdCall newInstance(DataEntityCollection type, String id) {
        FindByIdCall call = Records.newRecord(FindByIdCall.class);
        call.setEntityCollection(type);
        call.setId(id);
        return call;
    }

    public abstract String getId();

    public abstract void setId(String id);

    @Override
    public SingleEntityPayload execute(LockBasedDataStore dataStore, DatabaseReference db) {
        return SingleEntityPayload.newInstance(getEntityCollection(),
                dataStore.findById(db, getEntityCollection(), getId()));
    }
}
