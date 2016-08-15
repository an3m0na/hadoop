package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 8/15/16.
 */
public abstract class PolicyInfoMapPayload implements Payload{
    public static PolicyInfoMapPayload newInstance(Map<String, PolicyInfoPayload> entries) {
        PolicyInfoMapPayload map = Records.newRecord(PolicyInfoMapPayload.class);
        map.setEntries(entries);
        return map;
    }

    public abstract Map<String, PolicyInfoPayload> getEntries();

    public abstract void setEntries(Map<String, PolicyInfoPayload> entries);

}
