package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public abstract class DeleteByParamsCall extends DeleteCall {

    public static DeleteByParamsCall newInstance(DataEntityCollection type, Map<String, Object> params) {
        DeleteByParamsCall call = Records.newRecord(DeleteByParamsCall.class);
        call.setEntityCollection(type);
        call.setParams(params);
        return call;
    }

    public static DeleteByParamsCall newInstance(DataEntityDB db, DataEntityCollection type, Map<String, Object> params) {
        DeleteByParamsCall call = newInstance(type, params);
        call.setDatabase(db);
        return call;
    }

    public abstract Map<String, Object> getParams();

    public abstract void setParams(Map<String, Object> params);

    @Override
    public VoidPayload execute(DataStore dataStore) {
        dataStore.delete(getDatabase(), getEntityCollection(), getParams());
        return VoidPayload.newInstance();
    }
}
