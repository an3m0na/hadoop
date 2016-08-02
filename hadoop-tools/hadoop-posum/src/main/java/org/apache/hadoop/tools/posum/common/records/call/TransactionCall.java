package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;

import java.util.List;

/**
 * Created by ane on 7/30/16.
 */
public abstract class TransactionCall extends LockBasedDatabaseCallImpl<Payload> {

    public abstract DataEntityDB getEntityDBOrNull();

    public abstract void setEntityDBOrNull(DataEntityDB db);

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
        if (getEntityDBOrNull() != null)
            dataStore.lockForWrite(getEntityDBOrNull());
        else
            dataStore.lockForWrite();
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
        if (getEntityDBOrNull() != null)
            dataStore.unlockForWrite(getEntityDBOrNull());
        else
            dataStore.unlockForWrite();
    }
}
