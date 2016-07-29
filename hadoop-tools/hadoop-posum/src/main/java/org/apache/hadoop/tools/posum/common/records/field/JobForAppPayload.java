package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class JobForAppPayload {

    public static JobForAppPayload newInstance(DataEntityDB db, String appId, String user) {
        JobForAppPayload payload = Records.newRecord(JobForAppPayload.class);
        payload.setEntityDB(db);
        payload.setUser(user);
        payload.setAppId(appId);
        return payload;
    }

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract String getAppId();

    public abstract void setAppId(String id);

    public abstract String getUser();

    public abstract void setUser(String user);

}
