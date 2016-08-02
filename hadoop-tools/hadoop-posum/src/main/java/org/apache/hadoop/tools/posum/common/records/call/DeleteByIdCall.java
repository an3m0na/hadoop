package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class DeleteByIdCall extends DeleteCall {

    public static DeleteByIdCall newInstance(DataEntityDB db, DataEntityCollection type, String id) {
        DeleteByIdCall call = Records.newRecord(DeleteByIdCall.class);
        call.setEntityDB(db);
        call.setEntityCollection(type);
        call.setId(id);
        return call;
    }

    public abstract String getId();

    public abstract void setId(String id);

    @Override
    public VoidPayload execute() {
        dataStore.delete(getEntityDB(), getEntityCollection(), getId());
        return VoidPayload.newInstance();
    }
}
