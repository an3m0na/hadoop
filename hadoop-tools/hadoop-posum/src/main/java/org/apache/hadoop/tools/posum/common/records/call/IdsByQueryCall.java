package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.call.query.DatabaseQuery;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.StringListPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

public abstract class IdsByQueryCall extends ReadFromCollectionCall<StringListPayload> {

    public static IdsByQueryCall newInstance(DataEntityCollection collection, DatabaseQuery queryOrNull) {
        IdsByQueryCall call = Records.newRecord(IdsByQueryCall.class);
        call.setEntityCollection(collection);
        call.setQuery(queryOrNull);
        return call;
    }

    public static IdsByQueryCall newInstance(DataEntityCollection collection, DatabaseQuery queryOrNull, int offsetOrZero, int limitOrZero) {
        IdsByQueryCall call = newInstance(collection, queryOrNull);
        call.setOffsetOrZero(offsetOrZero);
        call.setLimitOrZero(limitOrZero);
        return call;
    }

    public static IdsByQueryCall newInstance(DataEntityCollection collection, DatabaseQuery queryOrNull, String sortField, boolean sortDescending) {
        IdsByQueryCall call = newInstance(collection, queryOrNull);
        call.setSortField(sortField);
        call.setSortDescending(sortDescending);
        return call;
    }

    public static IdsByQueryCall newInstance(DataEntityCollection collection, DatabaseQuery queryOrNull, String sortField, boolean sortDescending, int offsetOrZero, int limitOrZero) {
        IdsByQueryCall call = newInstance(collection, queryOrNull, offsetOrZero, limitOrZero);
        call.setSortField(sortField);
        call.setSortDescending(sortDescending);
        return call;
    }

    public abstract DatabaseQuery getQuery();

    public abstract void setQuery(DatabaseQuery queryOrNull);

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
                getQuery(),
                getSortField(),
                getSortDescending(),
                getOffsetOrZero(),
                getLimitOrZero()
        ));
    }
}
