package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.client.ExtendedDataClientInterface;

/**
 * Created by ane on 7/29/16.
 */
abstract class ThreePhaseDatabaseCallImpl<T extends Payload> implements ThreePhaseDatabaseCall<T> {
    protected ExtendedDataClientInterface dataStore;

    @Override
    public T executeCall(ExtendedDataClientInterface dataStore) {
        this.dataStore = dataStore;
        prepare();
        try {
            T ret = execute();
            commit();
            return ret;
        } catch (Exception e) {
            rollBack();
            throw e;
        }
    }
}
