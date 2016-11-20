package org.apache.hadoop.tools.posum.database.client;

import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;

/**
 * Created by ane on 8/3/16.
 */
public interface Database {
    <T extends Payload> T executeDatabaseCall(DatabaseCall<T> call);

    void clear();

    DataEntityDB getTarget();
}
