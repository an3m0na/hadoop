package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.database.store.DataStore;

/**
 * Created by ane on 7/29/16.
 */
public interface DatabaseCall<T extends Payload> {

    T executeCall(DataStore dataStore, DataEntityDB db);
}
