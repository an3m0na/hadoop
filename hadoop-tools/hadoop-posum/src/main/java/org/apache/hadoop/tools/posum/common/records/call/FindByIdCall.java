package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class FindByIdCall extends ReadFromCollectionCall<SingleEntityPayload> {

    public static FindByIdCall newInstance(DataEntityCollection type, String id) {
        FindByIdCall call = Records.newRecord(FindByIdCall.class);
        call.setEntityCollection(type);
        call.setId(id);
        return call;
    }

    public static FindByIdCall newInstance(DataEntityDB db, DataEntityCollection type, String id) {
        FindByIdCall call = newInstance(type, id);
        call.setDatabase(db);
        return call;
    }

    public abstract String getId();

    public abstract void setId(String id);

    @Override
    public SingleEntityPayload execute() {
        return SingleEntityPayload.newInstance(getEntityCollection(),
                dataStore.findById(getDatabase(), getEntityCollection(), getId()));
    }
}
