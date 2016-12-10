package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;

public interface ThreePhaseDatabaseCall<T extends Payload> extends DatabaseCall<T>{
    void prepare(LockBasedDataStore dataStore, DataEntityDB db);

    T execute(LockBasedDataStore dataStore, DataEntityDB db);

    void commit(LockBasedDataStore dataStore, DataEntityDB db);

    void rollBack(LockBasedDataStore dataStore, DataEntityDB db);
}
