package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 7/30/16.
 */
public abstract class TransactionCall extends LockBasedDatabaseCallImpl<Payload> {

    public static TransactionCall newInstance() {
        return Records.newRecord(TransactionCall.class);
    }

    public static TransactionCall newInstance(DataEntityDB db) {
        TransactionCall call = Records.newRecord(TransactionCall.class);
        call.setDatabase(db);
        return call;
    }

    public abstract List<ThreePhaseDatabaseCall> getCallList();

    public abstract void setCallList(List<? extends ThreePhaseDatabaseCall> callList);

    /**
     * Add a new database call to the call list
     *
     * @param call
     * @return the modified TransactionCall object (useful for chaining adds)
     */
    public abstract TransactionCall addCall(ThreePhaseDatabaseCall call);


    @Override
    public void lockDatabase(DataStore dataStore) {
        dataStore.lockForWrite(getDatabase());
    }

    @Override
    public Payload execute(DataStore dataStore) {
        Payload ret = VoidPayload.newInstance();
        for (ThreePhaseDatabaseCall call : getCallList()) {
            call.setDatabase(getDatabase());
            ret = call.execute(dataStore);
        }
        return ret;
    }

    @Override
    public void unlockDatabase(DataStore dataStore) {
        dataStore.unlockForWrite(getDatabase());
    }
}
