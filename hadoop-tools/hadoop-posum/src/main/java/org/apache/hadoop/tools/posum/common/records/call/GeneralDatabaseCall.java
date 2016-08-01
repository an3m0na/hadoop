package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.client.ExtendedDataClientInterface;

/**
 * Created by ane on 7/29/16.
 */
public abstract class GeneralDatabaseCall<T extends Payload> implements DatabaseCall<T> {
    protected ExtendedDataClientInterface dataStore;

    @Override
    public T executeCall(ExtendedDataClientInterface dataStore) {
        this.dataStore = dataStore;
        prepare();
        try {
            T ret = execute();
            commit();
            return ret;
        } finally {
            rollBack();
        }
    }

    protected abstract void prepare();

    protected abstract T execute();

    protected void commit() {
        wrapUp();
    }

    protected void rollBack() {
        wrapUp();
    }

    protected abstract void wrapUp();
}
