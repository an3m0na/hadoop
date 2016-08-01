package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public abstract class FindByParamsCall extends ReadCall<MultiEntityPayload> {

    public static FindByParamsCall newInstance(DataEntityDB db, DataEntityCollection collection, Map<String, Object> params) {
        FindByParamsCall call = Records.newRecord(FindByParamsCall.class);
        call.setEntityDB(db);
        call.setEntityCollection(collection);
        call.setParams(params);
        return call;
    }

    public static FindByParamsCall newInstance(DataEntityDB db, DataEntityCollection collection, Map<String, Object> params, int offsetOrZero, int limitOrZero) {
        FindByParamsCall call = newInstance(db, collection, params);
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
    protected MultiEntityPayload execute() {
        return MultiEntityPayload.newInstance(getEntityCollection(),
                dataStore.find(getEntityDB(), getEntityCollection(), getParams(), getOffsetOrZero(), getLimitOrZero()));
    }
}
