package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;

public interface DatabaseCall<T extends Payload> {

    T executeCall(LockBasedDataStore dataStore, DatabaseReference db);
}
