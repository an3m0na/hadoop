package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public abstract class IdsByParamsCall extends ReadFromCollectionCall<StringListPayload> {

    public static IdsByParamsCall newInstance(DataEntityCollection collection, Map<String, Object> params) {
        IdsByParamsCall call = Records.newRecord(IdsByParamsCall.class);
        call.setEntityCollection(collection);
        call.setParams(params);
        return call;
    }

    public static IdsByParamsCall newInstance(DataEntityCollection collection, Map<String, Object> params, int offsetOrZero, int limitOrZero) {
        IdsByParamsCall call = newInstance(collection, params);
        call.setOffsetOrZero(offsetOrZero);
        call.setLimitOrZero(limitOrZero);
        return call;
    }

    public abstract Map<String, Object> getParams();

    public abstract void setParams(Map<String, Object> params);

    public abstract int getLimitOrZero();

    public abstract void setLimitOrZero(int limitOrZero);

    public abstract int getOffsetOrZero();

    public abstract void setOffsetOrZero(int offsetOrZero);

    @Override
    public StringListPayload execute(DataStore dataStore, DataEntityDB db) {
        return StringListPayload.newInstance(dataStore.listIds(db, getEntityCollection(), getParams()));
    }
}
