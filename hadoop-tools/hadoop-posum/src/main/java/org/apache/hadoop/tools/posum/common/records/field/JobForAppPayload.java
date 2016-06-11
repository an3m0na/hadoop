package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityType;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */
public abstract class JobForAppPayload {

    public static JobForAppPayload newInstance(DataEntityDB db, String appId, String user) {
        JobForAppPayload request = Records.newRecord(JobForAppPayload.class);
        request.setEntityDB(db);
        request.setUser(user);
        request.setAppId(appId);
        return request;
    }

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract String getAppId();

    public abstract void setAppId(String id);

    public abstract String getUser();

    public abstract void setUser(String user);

}
