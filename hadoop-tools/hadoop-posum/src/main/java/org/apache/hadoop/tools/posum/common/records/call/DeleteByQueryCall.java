package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

public abstract class DeleteByQueryCall extends DeleteCall {

    public static DeleteByQueryCall newInstance(DataEntityCollection type, DatabaseQuery queryOrNull) {
        DeleteByQueryCall call = Records.newRecord(DeleteByQueryCall.class);
        call.setEntityCollection(type);
        call.setQuery(queryOrNull);
        return call;
    }

    public abstract DatabaseQuery getQuery();

    public abstract void setQuery(DatabaseQuery query);

    @Override
    public VoidPayload execute(LockBasedDataStore dataStore, DataEntityDB db) {
        dataStore.delete(db, getEntityCollection(), getQuery());
        return VoidPayload.newInstance();
    }
}
