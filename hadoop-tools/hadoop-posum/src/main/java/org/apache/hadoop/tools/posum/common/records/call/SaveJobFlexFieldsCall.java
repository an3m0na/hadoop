package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.payload.VoidPayload;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SaveJobFlexFieldsCall extends LockBasedDatabaseCallImpl<VoidPayload> {

    public static SaveJobFlexFieldsCall newInstance(String jobId, Map<String, String> newFields, boolean forHistory) {
        SaveJobFlexFieldsCall call = Records.newRecord(SaveJobFlexFieldsCall.class);
        call.setJobId(jobId);
        call.setNewFields(newFields);
        call.setForHistory(forHistory);
        return call;
    }

    public abstract String getJobId();

    public abstract void setJobId(String id);

    public abstract Map<String, String> getNewFields();

    public abstract void setNewFields(Map<String, String> newFields);

    public abstract boolean getForHistory();

    public abstract void setForHistory(boolean forHistory);

    @Override
    public VoidPayload execute(DataStore dataStore, DataEntityDB db) {
        dataStore.saveFlexFields(db, getJobId(), getNewFields(), getForHistory());
        return VoidPayload.newInstance();
    }

    @Override
    public void lockDatabase(DataStore dataStore, DataEntityDB db) {
        dataStore.lockForWrite(db);
    }

    @Override
    public void unlockDatabase(DataStore dataStore, DataEntityDB db) {
        dataStore.unlockForWrite(db);
    }
}
