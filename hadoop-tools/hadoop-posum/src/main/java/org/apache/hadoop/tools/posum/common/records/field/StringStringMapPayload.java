package org.apache.hadoop.tools.posum.common.records.field;

import org.apache.hadoop.yarn.util.Records;

import java.util.Map;

/**
 * Created by ane on 3/23/16.
 */
public abstract class StringStringMapPayload {

    public static StringStringMapPayload newInstance(Map<String, String> map) {
        StringStringMapPayload property = Records.newRecord(StringStringMapPayload.class);
        property.setEntries(map);
        return property;
    }

    public abstract Map<String, String> getEntries();

    public abstract void setEntries(Map<String, String> map);

}
