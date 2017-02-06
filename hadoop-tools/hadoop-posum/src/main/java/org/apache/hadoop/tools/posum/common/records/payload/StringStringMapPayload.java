package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

public abstract class StringStringMapPayload implements Payload{

    public static StringStringMapPayload newInstance(Map<String, String> map) {
        StringStringMapPayload payload = Records.newRecord(StringStringMapPayload.class);
        payload.setEntries(map);
        return payload;
    }

    public abstract void addEntry(String key, String value);

    public abstract Map<String, String> getEntries();

    public abstract void setEntries(Map<String, String> map);

}
