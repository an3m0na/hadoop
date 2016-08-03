package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;

import java.util.List;

/**
 * Created by ane on 7/30/16.
 */
public abstract class TransactionCall extends LockBasedDatabaseCallImpl<Payload> {

    public abstract List<ThreePhaseDatabaseCall> getCallList();

    public abstract void setCallList(List<ThreePhaseDatabaseCall> callList);

    /**
     * Add a new database call to the call list
     *
     * @param call
     * @return the modified TransactionCall object (useful for chaining adds)
     */
    public abstract TransactionCall addCall(ThreePhaseDatabaseCall call);


    @Override
    public void lockDatabase() {
        dataStore.lockForWrite(getDatabase());
    }

    @Override
    public Payload execute() {
        Payload ret = VoidPayload.newInstance();
        for (ThreePhaseDatabaseCall call : getCallList()) {
            ret = call.execute();
        }
        return ret;
    }

    @Override
    public void unlockDatabase() {
        dataStore.unlockForWrite(getDatabase());
    }
}
