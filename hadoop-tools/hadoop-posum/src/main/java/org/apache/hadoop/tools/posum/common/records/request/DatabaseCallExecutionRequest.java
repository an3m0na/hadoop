package org.apache.hadoop.tools.posum.common.records.request;


import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.yarn.util.Records;

/**
 * Created by ane on 3/20/16.
 */


public abstract class DatabaseCallExecutionRequest {

    public static DatabaseCallExecutionRequest newInstance(DatabaseCall call, DataEntityDB db) {
        DatabaseCallExecutionRequest request = Records.newRecord(DatabaseCallExecutionRequest.class);
        request.setEntityDB(db);
        request.setCall(call);
        return request;
    }

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract DatabaseCall getCall();

    public abstract void setCall(DatabaseCall call);
}
