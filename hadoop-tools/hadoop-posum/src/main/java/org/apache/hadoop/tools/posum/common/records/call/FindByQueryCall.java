package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.MultiEntityPayload;
import org.apache.hadoop.tools.posum.database.store.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class FindByQueryCall extends ReadFromCollectionCall<MultiEntityPayload> {

    public static FindByQueryCall newInstance(DataEntityCollection collection, DatabaseQuery query) {
        FindByQueryCall call = Records.newRecord(FindByQueryCall.class);
        call.setEntityCollection(collection);
        call.setQuery(query);
        return call;
    }

    public static FindByQueryCall newInstance(DataEntityCollection collection, DatabaseQuery params, int offsetOrZero, int limitOrZero) {
        FindByQueryCall call = newInstance(collection, params);
        call.setOffsetOrZero(offsetOrZero);
        call.setLimitOrZero(limitOrZero);
        return call;
    }

    public static FindByQueryCall newInstance(DataEntityCollection collection, DatabaseQuery params, String sortField, boolean sortDescending) {
        FindByQueryCall call = newInstance(collection, params);
        call.setSortField(sortField);
        call.setSortDescending(sortDescending);
        return call;
    }

    public static FindByQueryCall newInstance(DataEntityCollection collection, DatabaseQuery params, String sortField, boolean sortDescending, int offsetOrZero, int limitOrZero) {
        FindByQueryCall call = newInstance(collection, params, offsetOrZero, limitOrZero);
        call.setSortField(sortField);
        call.setSortDescending(sortDescending);
        return call;
    }

    public abstract DatabaseQuery getQuery();

    public abstract void setQuery(DatabaseQuery query);

    public abstract Integer getLimitOrZero();

    public abstract void setLimitOrZero(int limitOrZero);

    public abstract Integer getOffsetOrZero();

    public abstract void setOffsetOrZero(int offsetOrZero);

    public abstract String getSortField();

    public abstract void setSortField(String field);

    public abstract Boolean getSortDescending();

    public abstract void setSortDescending(boolean descending);

    @Override
    public MultiEntityPayload execute(LockBasedDataStore dataStore, DataEntityDB db) {
        return MultiEntityPayload.newInstance(getEntityCollection(), dataStore.find(
                db,
                getEntityCollection(),
                getQuery(),
                getSortField(),
                getSortDescending(),
                getOffsetOrZero(),
                getLimitOrZero()
        ));
    }
}
