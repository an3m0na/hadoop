package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;
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

    public static IdsByParamsCall newInstance(DataEntityCollection collection, Map<String, Object> params, String sortField, boolean sortDescending, int offsetOrZero, int limitOrZero) {
        IdsByParamsCall call = newInstance(collection, params, offsetOrZero, limitOrZero);
        call.setSortField(sortField);
        call.setSortDescending(sortDescending);
        return call;
    }

    public abstract Map<String, Object> getParams();

    public abstract void setParams(Map<String, Object> params);

    public abstract Integer getLimitOrZero();

    public abstract void setLimitOrZero(int limitOrZero);

    public abstract Integer getOffsetOrZero();

    public abstract void setOffsetOrZero(int offsetOrZero);

    public abstract String getSortField();

    public abstract void setSortField(String field);

    public abstract Boolean getSortDescending();

    public abstract void setSortDescending(boolean descending);

    @Override
    public StringListPayload execute(LockBasedDataStore dataStore, DataEntityDB db) {
        return StringListPayload.newInstance(dataStore.findIds(
                db,
                getEntityCollection(),
                getParams(),
                getSortField(),
                getSortDescending(),
                getOffsetOrZero(),
                getLimitOrZero()
        ));
    }
}
