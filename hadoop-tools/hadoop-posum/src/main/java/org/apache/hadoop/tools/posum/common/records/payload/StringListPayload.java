package org.apache.hadoop.tools.posum.common.records.payload;

import org.apache.hadoop.yarn.util.Records;

import java.util.List;

/**
 * Created by ane on 3/23/16.
 */
public abstract class StringListPayload implements Payload{

    public static StringListPayload newInstance(List<String> list) {
        StringListPayload payload = Records.newRecord(StringListPayload.class);
        payload.setEntries(list);
        return payload;
    }

    public abstract void addEntry( String value);

    public abstract List<String> getEntries();

    public abstract void setEntries(List<String> map);

}
