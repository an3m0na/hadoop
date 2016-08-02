package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;

/**
 * Created by ane on 8/2/16.
 */
abstract class LockBasedDatabaseCallImpl<T extends Payload> extends ThreePhaseDatabaseCallImpl<T> {


    public void prepare() {
        lockDatabase();
    }

    public void commit() {
        unlockDatabase();
    }

    public void rollBack() {
        unlockDatabase();
    }

    protected abstract void unlockDatabase();

    protected abstract void lockDatabase();
}
