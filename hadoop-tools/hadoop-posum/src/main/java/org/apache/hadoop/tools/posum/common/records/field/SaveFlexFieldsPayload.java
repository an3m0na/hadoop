package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.tools.posum.common.records.dataentity.DataEntityDB;
import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/20/16.
 */
public abstract class SaveFlexFieldsPayload {

    public static SaveFlexFieldsPayload newInstance(DataEntityDB db, String appId, Map<String, String> newFields, boolean forHistory) {
        SaveFlexFieldsPayload payload = Records.newRecord(SaveFlexFieldsPayload.class);
        payload.setEntityDB(db);
        payload.setJobId(appId);
        payload.setNewFields(newFields);
        payload.setForHistory(forHistory);
        return payload;
    }

    public abstract DataEntityDB getEntityDB();

    public abstract void setEntityDB(DataEntityDB db);

    public abstract String getJobId();

    public abstract void setJobId(String id);

    public abstract Map<String, String> getNewFields();

    public abstract void setNewFields(Map<String, String> newFields);

    public abstract boolean getForHistory();

    public abstract void setForHistory(boolean forHistory);

}
