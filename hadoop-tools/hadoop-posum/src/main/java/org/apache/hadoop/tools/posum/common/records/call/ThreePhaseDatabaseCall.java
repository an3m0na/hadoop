package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.DataStore;

/**
 * Created by ane on 8/2/16.
 */
public interface ThreePhaseDatabaseCall<T extends Payload> extends DatabaseCall<T>{
    void prepare(DataStore dataStore);

    T execute(DataStore dataStore);

    void commit(DataStore dataStore);

    void rollBack(DataStore dataStore);
}
