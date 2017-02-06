package org.apache.hadoop.tools.posum.common.records.request;


import org.apache.hadoop.tools.posum.common.records.call.DatabaseCall;
import org.apache.hadoop.tools.posum.common.records.dataentity.DatabaseReference;
import org.apache.hadoop.yarn.util.Records;



public abstract class DatabaseCallExecutionRequest {

    public static DatabaseCallExecutionRequest newInstance(DatabaseCall call, DatabaseReference db) {
        DatabaseCallExecutionRequest request = Records.newRecord(DatabaseCallExecutionRequest.class);
        request.setDatabase(db);
        request.setCall(call);
        return request;
    }

    public abstract DatabaseReference getDatabase();

    public abstract void setDatabase(DatabaseReference db);

    public abstract DatabaseCall getCall();

    public abstract void setCall(DatabaseCall call);
}
