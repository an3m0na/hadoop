package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;

/**
 * Created by ane on 8/2/16.
 */
public interface ThreePhaseDatabaseCall<T extends Payload> extends DatabaseCall<T>{
    void prepare();

    T execute();

    void commit();

    void rollBack();
}
