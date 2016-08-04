package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.DataStore;

/**
 * Created by ane on 8/2/16.
 */
public interface ThreePhaseDatabaseCall<T extends Payload> extends DatabaseCall<T>{
    void prepare(DataStore dataStore, DataEntityDB db);

    T execute(DataStore dataStore, DataEntityDB db);

    void commit(DataStore dataStore, DataEntityDB db);

    void rollBack(DataStore dataStore, DataEntityDB db);
}
