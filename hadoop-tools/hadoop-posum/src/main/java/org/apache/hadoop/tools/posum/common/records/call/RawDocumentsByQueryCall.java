package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.SimplePropertyPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

public abstract class RawDocumentsByQueryCall extends ReadFromCollectionCall<SimplePropertyPayload> {

    public static RawDocumentsByQueryCall newInstance(DataEntityCollection type, DatabaseQuery queryOrNull) {
        RawDocumentsByQueryCall call = Records.newRecord(RawDocumentsByQueryCall.class);
        call.setEntityCollection(type);
        call.setQuery(queryOrNull);
        return call;
    }

    public abstract DatabaseQuery getQuery();

    public abstract void setQuery(DatabaseQuery query);

    @Override
    public SimplePropertyPayload execute(LockBasedDataStore dataStore, DataEntityDB db) {
        String documents = dataStore.getRawDocuments(db, getEntityCollection(), getQuery());
        return SimplePropertyPayload.newInstance("", documents);
    }
}
