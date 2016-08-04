package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.DataStore;

/**
 * Created by ane on 7/29/16.
 */
abstract class ThreePhaseDatabaseCallImpl<T extends Payload> implements ThreePhaseDatabaseCall<T> {

    @Override
    public T executeCall(DataStore dataStore) {
        prepare(dataStore);
        try {
            T ret = execute(dataStore);
            commit(dataStore);
            return ret;
        } catch (Exception e) {
            rollBack(dataStore);
            throw e;
        }
    }
}
