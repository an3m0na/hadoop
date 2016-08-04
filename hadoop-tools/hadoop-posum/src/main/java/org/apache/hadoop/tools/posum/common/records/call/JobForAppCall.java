package org.apache.hadoop.tools.posum.common.records.call;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityCollection;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.JobProfile;
import org.apache.hadoop.tools.posum.common.records.payload.SingleEntityPayload;
import org.apache.hadoop.tools.posum.common.util.PosumException;
import org.apache.hadoop.tools.posum.database.store.DataStore;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;
import java.util.List;

/**
 * Created by ane on 8/2/16.
 */
public abstract class JobForAppCall extends LockBasedDatabaseCallImpl<SingleEntityPayload> {

    public static JobForAppCall newInstance(String appId, String user) {
        JobForAppCall call = Records.newRecord(JobForAppCall.class);
        call.setAppId(appId);
        call.setUser(user);
        return call;
    }

    public abstract String getAppId();

    public abstract void setAppId(String appId);

    public abstract String getUser();

    public abstract void setUser(String user);

    @Override
    public SingleEntityPayload execute(DataStore dataStore, DataEntityDB db) {
        FindByParamsCall findJobCall = FindByParamsCall.newInstance(DataEntityCollection.JOB,
                Collections.singletonMap("appId", (Object) getAppId()), 0, 0);
        List<JobProfile> profiles = findJobCall.executeCall(dataStore, db).getEntities();
        if (profiles.size() == 1)
            return SingleEntityPayload.newInstance(DataEntityCollection.JOB, profiles.get(0));
        if (profiles.size() > 1)
            throw new PosumException("Found too many profiles in database for app " + getAppId());
        return null;
    }

    @Override
    public void lockDatabase(DataStore dataStore, DataEntityDB db) {
        dataStore.lockForRead(db);
    }

    @Override
    public void unlockDatabase(DataStore dataStore, DataEntityDB db) {
        dataStore.unlockForRead(db);
    }

}
