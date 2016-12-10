package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.tools.posum.common.records.payload.Payload;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.tools.posum.data.core.LockBasedDataStore;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;

public abstract class TransactionCall extends LockBasedDatabaseCallImpl<Payload> {

    public static TransactionCall newInstance() {
        return Records.newRecord(TransactionCall.class);
    }

    public abstract List<ThreePhaseDatabaseCall> getCallList();

    public abstract void setCallList(List<? extends ThreePhaseDatabaseCall> callList);

    /**
     * Add a new database call to the call list
     *
     * @param call the call to be added
     * @return the modified TransactionCall object (useful for chaining adds)
     */
    public abstract TransactionCall addCall(ThreePhaseDatabaseCall call);

    /**
     * Add a set of Database calls to the call list
     *
     * @param calls the list of calls to be added
     * @return the modified TransactionCall object (useful for chaining adds)
     */
    public abstract TransactionCall addAllCalls(List<? extends ThreePhaseDatabaseCall> calls);


    @Override
    public void lockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
        dataStore.lockForWrite(db);
    }

    @Override
    public Payload execute(LockBasedDataStore dataStore, DatabaseReference db) {
        Payload ret = VoidPayload.newInstance();
        for (ThreePhaseDatabaseCall call : getCallList()) {
            ret = call.execute(dataStore, db);
        }
        return ret;
    }

    @Override
    public void unlockDatabase(LockBasedDataStore dataStore, DatabaseReference db) {
        dataStore.unlockForWrite(db);
    }
}
